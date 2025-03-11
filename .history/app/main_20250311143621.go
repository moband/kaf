package main

import (
	"encoding/binary"
	"fmt"
	"io"
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

	// First read the message size (4 bytes)
	messageSizeBytes := make([]byte, 4)
	_, err = io.ReadFull(conn, messageSizeBytes)
	if err != nil {
		fmt.Println("Error reading message size: ", err.Error())
		os.Exit(1)
	}

	messageSize := binary.BigEndian.Uint32(messageSizeBytes)

	// Read the rest of the message
	restOfMessage := make([]byte, messageSize)
	_, err = io.ReadFull(conn, restOfMessage)
	if err != nil {
		fmt.Println("Error reading message body: ", err.Error())
		os.Exit(1)
	}

	// Extract correlation ID (4 bytes after api_key and api_version)
	// In the request header, after the message_size (which we already read),
	// we have: 2 bytes api_key + 2 bytes api_version + 4 bytes correlation_id
	correlationID := binary.BigEndian.Uint32(restOfMessage[4:8])

	// Send Kafka response with the extracted correlation ID
	response := make([]byte, 8)

	// First 4 bytes: message_size (can be any value for this stage)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// Next 4 bytes: correlation_id (extracted from request)
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
