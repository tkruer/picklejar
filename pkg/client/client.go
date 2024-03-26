package client

import (
	"log"
	"net"

	"github.com/tkruer/picklejar/pkg/session"
)

type Client struct {
	Port string
}

func (c *Client) Start() {
	listener, err := net.Listen("tcp", c.Port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on tcp://0.0.0.0:%s", c.Port)

	for {
		conn, err := listener.Accept()
		log.Println("New connection", conn)
		if err != nil {
			log.Fatal(err)
		}

		go session.StartSession(conn)
	}
}

func (c *Client) Stop() {
	listener, err := net.Listen("tcp", c.Port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on tcp://0.0.0.0:%s", c.Port)

	for {
		conn, err := listener.Accept()
		log.Println("New connection", conn)
		if err != nil {
			log.Fatal(err)
		}
		// TODO: Implement a StopSession safe shutdown
		// go session.StopSession(conn)
	}
}
