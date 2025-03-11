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

	// Extract api_key (2 bytes)
	// apiKey := binary.BigEndian.Uint16(restOfMessage[0:2])

	// Extract api_version (2 bytes)
	apiVersion := binary.BigEndian.Uint16(restOfMessage[2:4])

	// Extract correlation ID (4 bytes after api_key and api_version)
	correlationID := binary.BigEndian.Uint32(restOfMessage[4:8])

	// Determine error code (0 if version supported, 35 if unsupported)
	var errorCode uint16 = 0
	if apiVersion > 4 {
		// 35 = UNSUPPORTED_VERSION
		errorCode = 35
	}

	// Send Kafka response with the extracted correlation ID and error code
	// Response format: 4 bytes message_size + 4 bytes correlation_id + 2 bytes error_code
	response := make([]byte, 10)

	// First 4 bytes: message_size (can be any value for this stage)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// Next 4 bytes: correlation_id (extracted from request)
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	// Next 2 bytes: error_code (0 if no error, 35 if unsupported version)
	binary.BigEndian.PutUint16(response[8:10], errorCode)

	// Define the response body for APIVersions
	responseBody := make([]byte, 6)

	// First 2 bytes: error_code (0 for no error)
	binary.BigEndian.PutUint16(responseBody[0:2], 0)

	// Next 2 bytes: API key (18 for API_VERSIONS)
	binary.BigEndian.PutUint16(responseBody[2:4], 18)

	// Next 2 bytes: MaxVersion (4 for this stage)
	binary.BigEndian.PutUint16(responseBody[4:6], 4)

	// Calculate the total message size (header + body)
	messageSize := uint32(4 + len(responseBody))

	// Update the response with the correct message size
	binary.BigEndian.PutUint32(response[0:4], messageSize)

	// Append the response body to the response
	response = append(response, responseBody...)

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
