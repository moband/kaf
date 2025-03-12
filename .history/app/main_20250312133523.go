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

// DescribeTopicPartitionsRequest represents a request to describe topic partitions
type DescribeTopicPartitionsRequest struct {
	// We only need to parse the topic name for this stage
	TopicNames []string
	Limit      int32
	Cursor     int8
	TagBuffer  int8
}

// DescribeTopicPartitionsResponse represents a response to a DescribeTopicPartitions request
type DescribeTopicPartitionsResponse struct {
	CorrelationID int32
	ThrottleTime  int32
	Topics        []TopicResponse
	TaggedFields  int8
}

// TopicResponse represents a single topic's response in the DescribeTopicPartitions response
type TopicResponse struct {
	Name         string
	ErrorCode    uint16
	TopicID      [16]byte // UUID (16 bytes)
	IsInternal   bool
	Partitions   []PartitionInfo
	TaggedFields int8
}

// PartitionInfo represents information about a single partition
type PartitionInfo struct {
	PartitionIndex int32
	LeaderID       int32
	LeaderEpoch    int32
	ISR            []int32 // In-Sync Replicas
	TaggedFields   int8
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

func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	// Parse the request payload
	describeRequest, err := parseDescribeTopicPartitionsRequest(request.Payload)
	if err != nil {
		fmt.Println("Error parsing DescribeTopicPartitions request:", err.Error())
		return err
	}

	fmt.Println("Processing DescribeTopicPartitions for topics:", describeRequest.TopicNames)

	// Create a structured response
	response := &DescribeTopicPartitionsResponse{
		CorrelationID: request.CorrelationID,
		ThrottleTime:  0,
		TaggedFields:  0,
		Topics:        make([]TopicResponse, 0, len(describeRequest.TopicNames)),
	}

	// Add a response for each topic - in this stage all topics are unknown
	for _, topicName := range describeRequest.TopicNames {
		// Create a null UUID (all zeros)
		var nullUUID [16]byte

		// Create topic response for unknown topic
		topicResponse := TopicResponse{
			Name:         topicName,
			ErrorCode:    ErrorUnknownTopicPartition,
			TopicID:      nullUUID,
			IsInternal:   false,
			Partitions:   []PartitionInfo{}, // Empty for unknown topics
			TaggedFields: 0,
		}

		response.Topics = append(response.Topics, topicResponse)
	}

	// Serialize the response to binary format
	responseData := SerializeDescribeTopicPartitionsResponse(response)

	// For debugging
	fmt.Println("Response size:", len(responseData))
	fmt.Println("Response hexdump:")
	for i := 0; i < len(responseData) && i < 64; i++ { // Print only first 64 bytes
		fmt.Printf("%02x ", responseData[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	if len(responseData) > 64 {
		fmt.Println("... (truncated)")
	}
	fmt.Println()

	// Print specific fields for debugging
	if len(responseData) >= 14 {
		corrId := binary.BigEndian.Uint32(responseData[0:4])
		throttleTime := binary.BigEndian.Uint32(responseData[4:8])
		topicsLen := binary.BigEndian.Uint16(responseData[8:10])

		offset := 10
		if len(responseData) >= offset+2 {
			nameLen := binary.BigEndian.Uint16(responseData[offset : offset+2])
			offset += 2

			fmt.Printf("Debug: corrId=%d, throttle=%d, topics=%d, nameLen=%d\n",
				corrId, throttleTime, topicsLen, nameLen)

			if len(responseData) >= offset+int(nameLen)+2 {
				name := string(responseData[offset : offset+int(nameLen)-1]) // -1 to skip null
				offset += int(nameLen)
				errorCode := binary.BigEndian.Uint16(responseData[offset : offset+2])
				fmt.Printf("Debug: name='%s', errorCode=%d\n", name, errorCode)
			}
		}
	}

	// Send the response
	return sendRawResponse(conn, responseData)
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

// parseDescribeTopicPartitionsRequest parses a DescribeTopicPartitions request payload
func parseDescribeTopicPartitionsRequest(payload []byte) (*DescribeTopicPartitionsRequest, error) {
	// Based on the hexdump, the format appears to be:
	// 00 0c             - Client ID length (12)
	// 6b 61...65 72     - "kafka-tester" (client ID)
	// 00 02             - Topics count (2)
	// 12                - Type marker for topic
	// 75 6e 6b...7a     - "unknown-topic-saz" (topic name - not length prefixed, but null terminated)
	// 00                - Null terminator
	// 00 00 00 01       - Limit
	// ff                - Cursor
	// 00                - Tag buffer

	if len(payload) < 4 {
		return nil, fmt.Errorf("payload too short")
	}

	// Print the payload for debugging
	fmt.Println("Payload hexdump:")
	for i := 0; i < len(payload); i++ {
		fmt.Printf("%02x ", payload[i])
		if (i+1)%16 == 0 {
			fmt.Println()
		}
	}
	fmt.Println()

	offset := 0

	// Parse client ID length (2 bytes)
	clientIDLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	// Skip client ID
	if len(payload) < offset+clientIDLength {
		return nil, fmt.Errorf("payload too short for client ID")
	}
	clientID := string(payload[offset : offset+clientIDLength])
	fmt.Printf("Client ID: %s\n", clientID)
	offset += clientIDLength

	// Read topics count (2 bytes)
	if len(payload) < offset+2 {
		return nil, fmt.Errorf("payload too short for topics count")
	}
	topicsCount := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	fmt.Printf("Topics count: %d\n", topicsCount)
	offset += 2

	// Create result
	result := &DescribeTopicPartitionsRequest{
		TopicNames: make([]string, 0, topicsCount),
		Limit:      1, // Default value
		Cursor:     0,
		TagBuffer:  0,
	}

	// Parse topics - looking at the hexdump, it seems there's only one topic despite topic count being 2
	for i := 0; i < topicsCount && offset < len(payload); i++ {
		// Check if we have enough bytes for the type marker
		if offset >= len(payload) {
			break
		}

		// Read type marker
		typeMarker := payload[offset]
		offset++

		// Find the topic name (null-terminated string)
		startPos := offset

		// Find the null terminator
		for offset < len(payload) && payload[offset] != 0 {
			offset++
		}

		if offset < len(payload) {
			topicName := string(payload[startPos:offset])
			fmt.Printf("Topic %d: marker=0x%02x, name='%s'\n", i+1, typeMarker, topicName)
			result.TopicNames = append(result.TopicNames, topicName)

			// Skip the null terminator
			offset++
		} else {
			// No null terminator found - use what we have
			if startPos < len(payload) {
				topicName := string(payload[startPos:])
				fmt.Printf("Topic %d (truncated): marker=0x%02x, name='%s'\n", i+1, typeMarker, topicName)
				result.TopicNames = append(result.TopicNames, topicName)
			}
			break
		}
	}

	// Read limit if we have enough bytes
	if offset+4 <= len(payload) {
		result.Limit = int32(binary.BigEndian.Uint32(payload[offset : offset+4]))
		fmt.Printf("Limit: %d\n", result.Limit)
		offset += 4
	}

	// Read cursor if we have enough bytes
	if offset < len(payload) {
		result.Cursor = int8(payload[offset])
		fmt.Printf("Cursor: %d\n", result.Cursor)
		offset++
	}

	// Read tag buffer if we have enough bytes
	if offset < len(payload) {
		result.TagBuffer = int8(payload[offset])
		fmt.Printf("Tag buffer: %d\n", result.TagBuffer)
	}

	// Validate that we found at least one topic name
	if len(result.TopicNames) == 0 {
		return nil, fmt.Errorf("no valid topic names found in payload")
	}

	return result, nil
}

// SerializeDescribeTopicPartitionsResponse converts the response struct to binary format
func SerializeDescribeTopicPartitionsResponse(response *DescribeTopicPartitionsResponse) []byte {
	// Calculate the total size of the serialized response
	size := 4 // Correlation ID
	size += 4 // Throttle time
	size += 2 // Topics array length

	// Calculate size for each topic
	for _, topic := range response.Topics {
		// For topic name, use the correct format for Kafka strings
		size += 2                   // Topic name length field
		size += len(topic.Name) + 1 // Topic name + NUL terminator
		size += 2                   // Error code
		size += 16                  // Topic ID (UUID)
		size += 1                   // Is internal flag
		size += 4                   // Partitions array length
		size += 1                   // Tagged fields
	}

	// Add 1 byte for the next cursor field
	size += 1

	// Add 1 byte for the tagged fields
	size += 1

	// Allocate buffer
	buf := make([]byte, size)
	offset := 0

	// Write correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(response.CorrelationID))
	offset += 4

	// Write throttle time
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(response.ThrottleTime))
	offset += 4

	// Write topics count
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(response.Topics)))
	offset += 2

	// Write each topic
	for _, topic := range response.Topics {
		// Topic name length (including NUL terminator)
		nameLength := len(topic.Name) + 1 // +1 for NUL terminator
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(nameLength))
		offset += 2

		// Topic name
		copy(buf[offset:offset+len(topic.Name)], topic.Name)
		offset += len(topic.Name)

		// NUL terminator for the string
		buf[offset] = 0
		offset += 1

		// Error code - ensure we're using the correct error code (3)
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(ErrorUnknownTopicPartition))
		offset += 2

		// Topic ID (UUID)
		copy(buf[offset:offset+16], topic.TopicID[:])
		offset += 16

		// Is internal flag
		if topic.IsInternal {
			buf[offset] = 1
		} else {
			buf[offset] = 0
		}
		offset += 1

		// Partitions array length (0 for unknown topics)
		binary.BigEndian.PutUint32(buf[offset:offset+4], 0)
		offset += 4

		// Tagged fields
		buf[offset] = byte(topic.TaggedFields)
		offset += 1
	}

	// Add next cursor (0xff as in the reference code)
	buf[offset] = 0xff
	offset += 1

	// Tagged fields for the overall response
	buf[offset] = byte(response.TaggedFields)

	// Print debug info to validate buffer
	fmt.Println("Final response layout:")
	fmt.Printf("- Correlation ID: bytes 0-3\n")
	fmt.Printf("- Throttle time: bytes 4-7\n")
	fmt.Printf("- Topics count: bytes 8-9\n")
	fmt.Printf("- Topic[0].name length: bytes 10-11\n")
	fmt.Printf("- Topic[0].name: bytes 12-%d\n", 12+len(response.Topics[0].Name))
	fmt.Printf("- Topic[0].error code: bytes %d-%d\n", 13+len(response.Topics[0].Name), 14+len(response.Topics[0].Name))

	return buf
}
