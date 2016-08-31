package main

import (
	"log"
	"os"

	"github.com/tstranex/gozmq"
)

func main() {
	c, err := gozmq.Subscribe(os.Args[1], []string{""})
	if err != nil {
		log.Fatalf("gozmq.Subscribe error: %v", err)
	}

	for {
		msg, err := c.Receive()
		if err != nil {
			log.Fatalf("Receive error: %v", err)
		}
		log.Printf("Received message: %+v", msg)
	}
}
