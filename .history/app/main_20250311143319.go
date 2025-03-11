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

	// Read request
	// We need at least the first 12 bytes to extract the correlation ID
	// 4 bytes message_size + 2 bytes api_key + 2 bytes api_version + 4 bytes correlation_id
	requestHeader := make([]byte, 12)
	_, err = io.ReadFull(conn, requestHeader)
	if err != nil {
		fmt.Println("Error reading request: ", err.Error())
		os.Exit(1)
	}

	// Extract correlation ID (bytes 8-12)
	correlationID := binary.BigEndian.Uint32(requestHeader[8:12])

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
