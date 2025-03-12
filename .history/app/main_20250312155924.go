package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Kafka protocol constants
const (
	// API Keys
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75

	// Error codes
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35
	ErrorUnknownTopic       uint16 = 3

	// Version constraints
	MinSupportedVersion int16 = 0
	MaxSupportedVersion int16 = 4

	// API version ranges
	ApiVersionsMinVersion   int16 = 0
	ApiVersionsMaxVersion   int16 = 4
	DescribeTopicMinVersion int16 = 0
	DescribeTopicMaxVersion int16 = 0
)

// KafkaRequest represents a Kafka protocol request
type KafkaRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	Payload       []byte
}

// KafkaResponse represents a generic Kafka protocol response
type KafkaResponse struct {
	CorrelationID int32
	ErrorCode     uint16
	Payload       []byte
}

func main() {
	fmt.Println("Kafka server starting...")

	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err.Error())
		os.Exit(1)
	}

	fmt.Println("Listening on port 9092")

	// Accept connections in a loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}

		fmt.Println("New connection from:", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Parse the incoming request
		request, err := readRequest(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading request:", err.Error())
			}
			return
		}

		// Log the request details
		logRequest(request)

		// Handle the request based on API key and version
		if err := handleRequest(conn, request); err != nil {
			fmt.Println("Error handling request:", err.Error())
			return
		}
	}
}

func readRequest(conn net.Conn) (*KafkaRequest, error) {
	request := &KafkaRequest{}

	// Read message length
	if err := binary.Read(conn, binary.BigEndian, &request.Length); err != nil {
		return nil, err
	}

	// Read API key
	if err := binary.Read(conn, binary.BigEndian, &request.ApiKey); err != nil {
		return nil, err
	}

	// Read API version
	if err := binary.Read(conn, binary.BigEndian, &request.ApiVersion); err != nil {
		return nil, err
	}

	// Read correlation ID
	if err := binary.Read(conn, binary.BigEndian, &request.CorrelationID); err != nil {
		return nil, err
	}

	// Read the rest of the request payload
	request.Payload = make([]byte, request.Length-8) // 8 bytes for ApiKey, ApiVersion, and CorrelationID
	if err := binary.Read(conn, binary.BigEndian, &request.Payload); err != nil {
		return nil, err
	}

	return request, nil
}

func logRequest(request *KafkaRequest) {
	fmt.Println("Request details:")
	fmt.Println(" Length:", request.Length)
	fmt.Println(" API Key:", request.ApiKey)
	fmt.Println(" API Version:", request.ApiVersion)
	fmt.Println(" Correlation ID:", request.CorrelationID)
}

func handleRequest(conn net.Conn, request *KafkaRequest) error {
	// Check if API version is supported
	if request.ApiVersion < MinSupportedVersion || request.ApiVersion > MaxSupportedVersion {
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnsupportedVersion)
	}

	// Handle the request based on the API key
	switch request.ApiKey {
	case ApiVersionsKey:
		return handleApiVersionsRequest(conn, request)
	case DescribeTopicPartitionsKey:
		return handleDescribeTopicPartitionsRequest(conn, request)
	default:
		return handleGenericRequest(conn, request)
	}
}

func handleApiVersionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Create response for API_VERSIONS request
	response := make([]byte, 26)

	// Correlation ID
	binary.BigEndian.PutUint32(response[0:4], uint32(request.CorrelationID))

	// Error code (0 for success)
	binary.BigEndian.PutUint16(response[4:6], ErrorNone)

	// Number of API keys in response
	response[6] = 3

	// API Key 1 - API_VERSIONS (18)
	binary.BigEndian.PutUint16(response[7:9], uint16(ApiVersionsKey))
	binary.BigEndian.PutUint16(response[9:11], uint16(ApiVersionsMinVersion))  // Min version
	binary.BigEndian.PutUint16(response[11:13], uint16(ApiVersionsMaxVersion)) // Max version
	response[13] = 0                                                           // _tagged_fields

	// API Key 2 - DescribeTopicPartitions (75)
	binary.BigEndian.PutUint16(response[14:16], uint16(DescribeTopicPartitionsKey))
	binary.BigEndian.PutUint16(response[16:18], uint16(DescribeTopicMinVersion)) // Min version
	binary.BigEndian.PutUint16(response[18:20], uint16(DescribeTopicMaxVersion)) // Max version
	response[20] = 0                                                             // _tagged_fields

	// Throttle time
	binary.BigEndian.PutUint32(response[21:25], 0)

	// _tagged_fields for the overall response
	response[25] = 0

	return sendRawResponse(conn, response)
}

func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Parse the request
	// The first part of the payload contains the client ID (string) followed by a tag buffer
	offset := 0

	// Skip the client ID
	if offset >= len(request.Payload) {
		return fmt.Errorf("unexpected end of payload when reading client ID length")
	}

	// Client ID is a string with 2-byte length prefix
	clientIDLength := int(binary.BigEndian.Uint16(request.Payload[offset : offset+2]))
	offset += 2 + clientIDLength

	// Skip the client ID tag buffer (1 byte)
	if offset >= len(request.Payload) {
		return fmt.Errorf("unexpected end of payload when reading client ID tag buffer")
	}
	offset++

	// Now we're at the topics array
	// First byte indicates the array length (varint encoding: actual length + 1)
	if offset >= len(request.Payload) {
		return fmt.Errorf("unexpected end of payload when reading topics array length")
	}

	topicArrayLength := int(request.Payload[offset]) - 1
	if topicArrayLength < 0 {
		return fmt.Errorf("invalid topics array length")
	}
	offset++

	// Start building the response
	// Estimate size - we'll resize later if needed
	responseSize := 12 + (40 * topicArrayLength) // rough estimate: header + topics array
	response := make([]byte, responseSize)

	// Response header
	// Correlation ID
	binary.BigEndian.PutUint32(response[0:4], uint32(request.CorrelationID))

	// Tag buffer for the header (empty)
	response[4] = 0

	// Throttle time (0)
	binary.BigEndian.PutUint32(response[5:9], 0)

	// Topics array length (compact array encoding)
	response[9] = byte(topicArrayLength + 1) // varint encoding of array length + 1

	// Parse and process each topic in the request
	responseOffset := 10 // Start after the topics array length in response

	for i := 0; i < topicArrayLength; i++ {
		// Extract the topic name length (varint)
		if offset >= len(request.Payload) {
			return fmt.Errorf("unexpected end of payload when reading topic name length")
		}

		topicNameLength := int(request.Payload[offset]) - 1
		offset++

		// Extract the topic name
		if offset+topicNameLength > len(request.Payload) {
			return fmt.Errorf("invalid topic name length or unexpected end of payload")
		}

		topicName := string(request.Payload[offset : offset+topicNameLength])
		offset += topicNameLength

		// Skip any tag buffer at the end of the topic entry
		if offset < len(request.Payload) && request.Payload[offset] == 0 {
			offset++
		}

		// Now add this topic to the response

		// Topic error code - UNKNOWN_TOPIC (3)
		if responseOffset+2 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		binary.BigEndian.PutUint16(response[responseOffset:responseOffset+2], ErrorUnknownTopic)
		responseOffset += 2

		// Topic name (compact string)
		if responseOffset+1+len(topicName) > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = byte(len(topicName) + 1) // varint encoding of string length + 1
		responseOffset++
		copy(response[responseOffset:responseOffset+len(topicName)], topicName)
		responseOffset += len(topicName)

		// Topic ID (all zeros for unknown topic - 16 bytes)
		if responseOffset+16 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		// No need to set these explicitly as the buffer is already zeroed
		responseOffset += 16

		// is_internal (false)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 0
		responseOffset++

		// Partitions array (empty)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 1 // varint encoding of array length + 1 (empty array)
		responseOffset++

		// Topic authorized operations
		if responseOffset+4 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		binary.BigEndian.PutUint32(response[responseOffset:responseOffset+4], 0x00000df8)
		responseOffset += 4

		// Tag buffer for the topic (empty)
		if responseOffset+1 > len(response) {
			// Expand response buffer if needed
			newResponse := make([]byte, len(response)*2)
			copy(newResponse, response)
			response = newResponse
		}
		response[responseOffset] = 0
		responseOffset++
	}

	// Skip the response partition limit and cursor in the request
	// (We don't need these values for the response)

	// Next cursor (null)
	if responseOffset+1 > len(response) {
		// Expand response buffer if needed
		newResponse := make([]byte, len(response)*2)
		copy(newResponse, response)
		response = newResponse
	}
	response[responseOffset] = 0xff
	responseOffset++

	// Tag buffer (empty)
	if responseOffset+1 > len(response) {
		// Expand response buffer if needed
		newResponse := make([]byte, len(response)*2)
		copy(newResponse, response)
		response = newResponse
	}
	response[responseOffset] = 0
	responseOffset++

	// Trim the response to the actual size used
	return sendRawResponse(conn, response[:responseOffset])
}

func handleGenericRequest(conn net.Conn, request *KafkaRequest) error {
	// Simple response for other API keys
	response := &KafkaResponse{
		CorrelationID: request.CorrelationID,
		ErrorCode:     ErrorNone,
	}

	return sendResponse(conn, response)
}

func sendErrorResponse(conn net.Conn, correlationID int32, errorCode uint16) error {
	// Create minimal error response
	response := make([]byte, 6)
	binary.BigEndian.PutUint32(response[0:4], uint32(correlationID))
	binary.BigEndian.PutUint16(response[4:6], errorCode)

	return sendRawResponse(conn, response)
}

func sendResponse(conn net.Conn, response *KafkaResponse) error {
	// Calculate response size
	responseSize := 6 // 4 bytes for correlationID + 2 bytes for errorCode
	if response.Payload != nil {
		responseSize += len(response.Payload)
	}

	// Create response buffer
	buffer := make([]byte, responseSize)

	// Write correlation ID
	binary.BigEndian.PutUint32(buffer[0:4], uint32(response.CorrelationID))

	// Write error code
	binary.BigEndian.PutUint16(buffer[4:6], response.ErrorCode)

	// Write payload if present
	if response.Payload != nil {
		copy(buffer[6:], response.Payload)
	}

	return sendRawResponse(conn, buffer)
}

func sendRawResponse(conn net.Conn, data []byte) error {
	// Write message size
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}

	// Write message data
	if err := binary.Write(conn, binary.BigEndian, data); err != nil {
		return err
	}

	return nil
}
