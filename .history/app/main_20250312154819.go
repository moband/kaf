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
	ErrorNone                  uint16 = 0
	ErrorUnsupportedVersion    uint16 = 35
	ErrorUnknownTopicPartition uint16 = 3

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
	fmt.Println("  Length:", request.Length)
	fmt.Println("  API Key:", request.ApiKey)
	fmt.Println("  API Version:", request.ApiVersion)
	fmt.Println("  Correlation ID:", request.CorrelationID)
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

// parseCompactString parses a compact string from a byte slice
// and returns the string and the number of bytes consumed
func parseCompactString(data []byte) (string, int, error) {
	if len(data) < 1 {
		return "", 0, fmt.Errorf("data too short for compact string")
	}

	// Parse length (varint - first byte)
	length := int(data[0]) - 1
	if length < 0 {
		return "", 1, fmt.Errorf("invalid compact string length")
	}

	if len(data) < 1+length {
		return "", 0, fmt.Errorf("data too short for compact string content")
	}

	return string(data[1 : 1+length]), 1 + length, nil
}

func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Parse the client ID (compact string)
	payload := request.Payload
	var clientID string
	var bytesRead int
	var err error

	// Skip client ID
	clientID, bytesRead, err = parseCompactString(payload)
	if err != nil {
		fmt.Println("Error parsing client ID:", err)
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
	}

	fmt.Println("Client ID:", clientID)
	payload = payload[bytesRead:]

	// Skip tag buffer
	if len(payload) < 1 {
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
	}
	payload = payload[1:] // Skip tag buffer byte

	// Parse topic array
	if len(payload) < 1 {
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
	}

	// Topic array length
	arrayLength := int(payload[0]) - 1
	if arrayLength < 0 {
		return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
	}
	payload = payload[1:]

	// We only care about the first topic for now
	topicName := ""
	if arrayLength > 0 {
		topicName, bytesRead, err = parseCompactString(payload)
		if err != nil {
			fmt.Println("Error parsing topic name:", err)
			return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
		}
		fmt.Println("Topic name:", topicName)

		// Skip tag buffer for the topic
		if len(payload) < bytesRead+1 {
			return sendErrorResponse(conn, request.CorrelationID, ErrorUnknownTopicPartition)
		}
		payload = payload[bytesRead+1:] // Skip topic string and its tag buffer
	}

	// Skip response partition limit and cursor
	// We don't need to use these values now, but we are correctly parsing the request

	// Construct response for unknown topic
	return sendDescribeTopicPartitionsResponse(conn, request.CorrelationID, topicName)
}

func sendDescribeTopicPartitionsResponse(conn net.Conn, correlationID int32, topicName string) error {
	// Calculate response size based on the protocol
	topicNameBytes := []byte(topicName)
	topicNameLength := len(topicNameBytes)

	// Response construction
	response := make([]byte, 0, 100) // Pre-allocate enough space

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, uint32(correlationID))
	response = append(response, correlationIDBytes...)

	// Tag buffer for response header (1 byte)
	response = append(response, 0)

	// Throttle time (4 bytes)
	throttleTimeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeBytes, 0)
	response = append(response, throttleTimeBytes...)

	// Topics array length (varint - 1 byte in this case)
	response = append(response, byte(2)) // Length + 1 = 2 (array with 1 element)

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, ErrorUnknownTopicPartition)
	response = append(response, errorCodeBytes...)

	// Topic name (compact string)
	response = append(response, byte(topicNameLength+1)) // Length + 1
	response = append(response, topicNameBytes...)

	// Topic ID (UUID - 16 bytes of zeros)
	response = append(response, make([]byte, 16)...)

	// Is internal (1 byte)
	response = append(response, 0) // false

	// Partitions array (empty - 1 byte)
	response = append(response, 1) // Length + 1 = 1 (empty array)

	// Topic authorized operations (4 bytes)
	authOpsBytes := make([]byte, 4)
	// Set authorized operations: 0x00000df8 (READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, DESCRIBE_CONFIGS, ALTER_CONFIGS)
	binary.BigEndian.PutUint32(authOpsBytes, 0x00000df8)
	response = append(response, authOpsBytes...)

	// Tag buffer for topic (1 byte)
	response = append(response, 0)

	// Next cursor (nullable - 1 byte)
	response = append(response, 0xff) // null

	// Tag buffer for response (1 byte)
	response = append(response, 0)

	// Send the response
	return sendRawResponse(conn, response)
}
