// Package gozmq provides a ZMQ pubsub client.
//
// It implements the protocol described here:
// http://rfc.zeromq.org/spec:23/ZMTP/
package gozmq

import (
	"errors"
	"io"
	"net"
	"time"
)

func writeAll(w io.Writer, buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := w.Write(buf[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func writeGreeting(w io.Writer) error {
	signature := []byte{0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f}
	version := []byte{3, 0}
	mechanism := make([]byte, 20)
	for i, c := range "NULL" {
		mechanism[i] = byte(c)
	}
	server := []byte{0}

	var greeting []byte
	greeting = append(greeting, signature...)
	greeting = append(greeting, version...)
	greeting = append(greeting, mechanism...)
	greeting = append(greeting, server...)
	for len(greeting) < 64 {
		greeting = append(greeting, 0)
	}

	if err := writeAll(w, greeting); err != nil {
		return err
	}

	return nil
}

func readGreeting(r io.Reader) error {
	greet := make([]byte, 64)
	if _, err := io.ReadFull(r, greet); err != nil {
		return err
	}

	if greet[0] != 0xff || greet[9] != 0x7f {
		return errors.New("invalid signature")
	}
	if greet[10] < 3 {
		return errors.New("peer version is too old")
	}
	if string(greet[12:17]) != "NULL\x00" {
		return errors.New("unsupported security mechanism")
	}
	if greet[32] != 0 {
		return errors.New("as-server must be zero for NULL")
	}

	return nil
}

func readFrame(r io.Reader) (byte, []byte, error) {
	var flagBuf [1]byte
	if _, err := io.ReadFull(r, flagBuf[:1]); err != nil {
		return 0, nil, err
	}

	flag := flagBuf[0]
	if flag&0xf8 != 0 {
		return 0, nil, errors.New("invalid flag")
	}

	var size uint64

	if flag&2 == 2 {
		// Long form
		var buf [8]byte
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return 0, nil, err
		}
		for _, b := range buf {
			size = (size << 8) | uint64(b)
		}
	} else {
		// Short form
		var buf [1]byte
		if _, err := io.ReadFull(r, buf[:1]); err != nil {
			return 0, nil, err
		}
		size = uint64(buf[0])
	}

	if size > 1024*1024 {
		return 0, nil, errors.New("frame too large")
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}

	return flag, buf, nil
}

func writeFrame(w io.Writer, flag byte, buf []byte) error {
	if flag&0xf8 != 0 {
		return errors.New("invalid flag")
	}

	if flag&2 != 0 {
		return errors.New("caller must not specify long frame flag")
	}

	var header []byte

	if len(buf) > 255 {
		flag = flag | 2
		header = []byte{flag, 0, 0, 0, 0, 0, 0, 0, 0}
		size := len(buf)
		i := len(header) - 1
		for size > 0 {
			header[i] = byte(size & 0xff)
			size = size >> 8
			i--
		}
	} else {
		header = []byte{flag, byte(len(buf))}
	}

	if err := writeAll(w, header); err != nil {
		return err
	}
	return writeAll(w, buf)
}

func writeCommand(w io.Writer, name string, data []byte) error {
	size := len(name)
	if size > 255 {
		return errors.New("command name is too long")
	}
	body := append([]byte{byte(size)}, []byte(name)...)
	buf := append(body, data...)
	var flag byte = 4
	return writeFrame(w, flag, buf)
}

func readCommand(r io.Reader) (string, []byte, error) {
	flag, buf, err := readFrame(r)
	if err != nil {
		return "", nil, err
	}
	if flag&4 != 4 {
		return "", nil, errors.New("expected command frame")
	}
	if len(buf) < 1 {
		return "", nil, errors.New("empty command buffer")
	}
	size := int(buf[0])
	buf = buf[1:]
	if size > len(buf) {
		return "", nil, errors.New("invalid command name size")
	}
	name := string(buf[:size])
	data := buf[size:]
	return name, data, nil
}

const commandReady = "READY"

func readReady(r io.Reader) error {
	name, metadata, err := readCommand(r)
	if err != nil {
		return err
	}
	if name != commandReady {
		return errors.New("expected ready command")
	}

	m := make(map[string]string)
	for len(metadata) > 0 {
		size := int(metadata[0])
		metadata = metadata[1:]
		if size > len(metadata) {
			return errors.New("invalid metadata")
		}
		name := metadata[:size]
		metadata = metadata[size:]

		if len(metadata) < 4 {
			return errors.New("invalid metadata")
		}
		var valueSize uint32
		for i := 0; i < 4; i++ {
			valueSize = valueSize<<8 + uint32(metadata[i])
		}
		metadata = metadata[4:]

		if int(valueSize) > len(metadata) {
			return errors.New("invalid metadata")
		}
		value := metadata[:valueSize]
		metadata = metadata[valueSize:]

		m[string(name)] = string(value)
	}

	return nil
}

func writeReady(w io.Writer, socketType string) error {
	if len(socketType) > 255 {
		return errors.New("socket type too long")
	}
	const socketTypeName = "Socket-Type"
	metadata := []byte{byte(len(socketTypeName))}
	metadata = append(metadata, []byte(socketTypeName)...)
	metadata = append(metadata, []byte{0, 0, 0, byte(len(socketType))}...)
	metadata = append(metadata, []byte(socketType)...)
	return writeCommand(w, commandReady, metadata)
}

func writeMessage(w io.Writer, parts [][]byte) error {
	if len(parts) == 0 {
		return errors.New("empty message")
	}
	for _, msg := range parts[:len(parts)-1] {
		if err := writeFrame(w, 1, msg); err != nil {
			return err
		}
	}
	return writeFrame(w, 0, parts[len(parts)-1])
}

func readMessage(r io.Reader) ([][]byte, error) {
	var parts [][]byte
	for {
		flag, buf, err := readFrame(r)
		if err != nil {
			return nil, err
		}
		if flag&4 != 0 {
			return nil, errors.New("expected message frame")
		}

		parts = append(parts, buf)

		if flag&1 == 0 {
			break
		}

		if len(parts) > 16 {
			return nil, errors.New("message has too many parts")
		}
	}
	return parts, nil
}

func subscribe(w io.Writer, prefix string) error {
	msg := append([]byte{1}, []byte(prefix)...)
	return writeMessage(w, [][]byte{msg})
}

// Conn is a connection to a ZMQ server.
type Conn struct {
	conn net.Conn
}

// Subscribe connects to a publisher server and subscribes to the given topics.
func Subscribe(addr string, topics []string) (*Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Minute)
	if err != nil {
		return nil, err
	}

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	if err := writeGreeting(conn); err != nil {
		conn.Close()
		return nil, err
	}
	if err := readGreeting(conn); err != nil {
		conn.Close()
		return nil, err
	}

	if err := writeReady(conn, "SUB"); err != nil {
		conn.Close()
		return nil, err
	}
	if err := readReady(conn); err != nil {
		conn.Close()
		return nil, err
	}

	for _, topic := range topics {
		if err := subscribe(conn, topic); err != nil {
			conn.Close()
			return nil, err
		}
	}

	conn.SetDeadline(time.Time{})

	return &Conn{conn}, nil
}

// Receive a message from the publisher. It blocks until a new message is
// received.
func (c *Conn) Receive() ([][]byte, error) {
	return readMessage(c.conn)
}

// Close the underlying connection. Any further operations will fail.
func (c *Conn) Close() error {
	return c.conn.Close()
}
