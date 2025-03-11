package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

// Kafka protocol constants
const (
	// API Keys
	ApiVersionsKey uint16 = 18

	// Error codes
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35
)

// handleConnection processes a single Kafka client connection
func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the message size (4 bytes)
	var length int32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		log.Printf("Error reading message size: %v", err)
		return
	}

	// Read the API key (2 bytes)
	var requestApiKey int16
	if err := binary.Read(conn, binary.BigEndian, &requestApiKey); err != nil {
		log.Printf("Error reading API key: %v", err)
		return
	}

	// Read the API version (2 bytes)
	var requestApiVersion int16
	if err := binary.Read(conn, binary.BigEndian, &requestApiVersion); err != nil {
		log.Printf("Error reading API version: %v", err)
		return
	}

	// Read the correlation ID (4 bytes)
	var correlationId int32
	if err := binary.Read(conn, binary.BigEndian, &correlationId); err != nil {
		log.Printf("Error reading correlation ID: %v", err)
		return
	}

	// Read the rest of the message
	rest := make([]byte, length-8)
	if _, err := io.ReadFull(conn, rest); err != nil {
		log.Printf("Error reading rest of message: %v", err)
		return
	}

	// Determine if the API version is supported
	if requestApiVersion < 0 || requestApiVersion > 4 {
		errorResponse := make([]byte, 6)
		binary.BigEndian.PutUint32(errorResponse, uint32(correlationId))
		binary.BigEndian.PutUint16(errorResponse[4:], ErrorUnsupportedVersion)
		send(conn, errorResponse)
		return
	}

	// Create successful response
	out := make([]byte, 19)

	// Fill in correlation ID
	binary.BigEndian.PutUint32(out, uint32(correlationId))

	// Fill in error code (2 bytes)
	binary.BigEndian.PutUint16(out[4:], ErrorNone)

	// Number of API keys (1 byte)
	out[6] = 1

	// API key entry for ApiVersions
	binary.BigEndian.PutUint16(out[7:], ApiVersionsKey) // API key (18)
	binary.BigEndian.PutUint16(out[9:], 0)              // Min version (0)
	binary.BigEndian.PutUint16(out[11:], 4)             // Max version (4)

	// Tagged fields for the API entry
	out[13] = 0

	// Throttle time (4 bytes) - placed near the end
	binary.BigEndian.PutUint32(out[14:], 0)

	// Tagged fields at the end
	out[18] = 0

	// Send the response
	send(conn, out)
}

// send writes the message size followed by the message content
func send(conn net.Conn, data []byte) error {
	// Write message size (4 bytes)
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return fmt.Errorf("failed to write message size: %w", err)
	}

	// Write message content
	if err := binary.Write(conn, binary.BigEndian, data); err != nil {
		return fmt.Errorf("failed to write message content: %w", err)
	}

	return nil
}

func main() {
	// Listen for connections
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to bind to port 9092: %v", err)
	}
	defer listener.Close()

	log.Println("Kafka broker listening on port 9092")

	// Accept and process one connection (for this challenge)
	conn, err := listener.Accept()
	if err != nil {
		log.Fatalf("Error accepting connection: %v", err)
	}

	handleConnection(conn)
}
