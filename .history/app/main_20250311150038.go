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

	// Structure of the APIVersions response:
	// Header: 4 bytes message_size + 4 bytes correlation_id
	// Body: 2 bytes error_code + 4 bytes throttle_time_ms + APIVersion array

	// For the APIVersion array, we need:
	// 1-byte array length (compact array format) - we'll include just 1 entry
	// For each entry: 2 bytes api_key + 2 bytes min_version + 2 bytes max_version
	// Then 1 byte for tagged fields (0 means no tagged fields)

	// Calculate sizes
	const headerSize = 8 // 4 bytes message_size + 4 bytes correlation_id
	const bodySize = 10  // 2 bytes error_code + 4 bytes throttle_time_ms + 1 byte array length + 2 bytes api_key + 2 bytes min_version + 2 bytes max_version + 1 byte tagged fields
	const responseSize = headerSize + bodySize

	// Create response buffer
	response := make([]byte, responseSize)

	// Fill in header
	// First 4 bytes: message_size (total size - 4 bytes for message_size itself)
	binary.BigEndian.PutUint32(response[0:4], uint32(bodySize))

	// Next 4 bytes: correlation_id (extracted from request)
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	// Fill in body
	// 2 bytes: error_code (0 if no error, 35 if unsupported version)
	binary.BigEndian.PutUint16(response[8:10], errorCode)

	// 4 bytes: throttle_time_ms (0 for no throttling)
	binary.BigEndian.PutUint32(response[10:14], 0)

	// 1 byte: array length (1 entry in the array)
	response[14] = 1

	// For entry:
	// 2 bytes: api_key (18 for APIVersions)
	binary.BigEndian.PutUint16(response[15:17], 18)

	// 2 bytes: min_version (0)
	binary.BigEndian.PutUint16(response[17:19], 0)

	// 2 bytes: max_version (4)
	binary.BigEndian.PutUint16(response[19:21], 4)

	// 1 byte: tagged fields (0 for no tagged fields)
	response[21] = 0

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
