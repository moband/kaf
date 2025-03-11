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
	// Response format:
	// - 4 bytes message_size
	// - 4 bytes correlation_id
	// - 4 bytes throttle_time_ms
	// - 2 bytes error_code
	// - 4 bytes api_keys array length
	// - For each api key:
	//   - 2 bytes api_key
	//   - 2 bytes min_version
	//   - 2 bytes max_version
	response := make([]byte, 18)

	// First 4 bytes: message_size
	binary.BigEndian.PutUint32(response[0:4], 14) // 4 (correlation_id) + 4 (throttle_time_ms) + 2 (error_code) + 4 (array length)

	// Next 4 bytes: correlation_id
	binary.BigEndian.PutUint32(response[4:8], correlationID)

	// Next 4 bytes: throttle_time_ms (0 for no throttling)
	binary.BigEndian.PutUint32(response[8:12], 0)

	// Next 2 bytes: error_code
	binary.BigEndian.PutUint16(response[12:14], errorCode)

	// Next 4 bytes: api_keys array length (1 entry)
	binary.BigEndian.PutUint32(response[14:18], 1)

	// Add the API_VERSIONS entry
	apiEntry := make([]byte, 6)
	binary.BigEndian.PutUint16(apiEntry[0:2], 18) // API key 18 (API_VERSIONS)
	binary.BigEndian.PutUint16(apiEntry[2:4], 0)  // Min version
	binary.BigEndian.PutUint16(apiEntry[4:6], 4)  // Max version

	// Append the API entry
	response = append(response, apiEntry...)

	// Update the final message size
	binary.BigEndian.PutUint32(response[0:4], uint32(len(response)-4)) // subtract the 4 bytes of message_size itself

	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending response: ", err.Error())
		os.Exit(1)
	}

	conn.Close()
}
