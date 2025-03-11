package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	// Send Kafka response with correlation ID
	response := make([]byte, 8)

	// First 4 bytes: message_size (can be any value for this stage)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// Next 4 bytes: correlation_id (must be 7)
	binary.BigEndian.PutUint32(response[4:8], 7)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
