package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"golang.org/x/exp/constraints"
)

// Kafka protocol constants
const (
	// API Keys
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75

	// Error codes
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35

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

func handleDescribeTopicPartitionsRequest(conn net.Conn, request *KafkaRequest) error {
	topicNames := parseTopicNames(request.Payload)

	relevantTopics := func(topics map[string]Topic, names []string) (filteredTopics []Topic) {
		for _, name := range names {
			topic, ok := topics[name]
			if ok {
				filteredTopics = append(filteredTopics, topic)
			}
		}

		return filteredTopics
	}(parsedTopics, topicNames)

	var responseBody DescribeTopicPartitionsResponse
	if len(relevantTopics) == 0 {
		responseBody = DescribeTopicPartitionsResponse{
			topics: []Topic{
				{
					errorCode: 3,
					name: CompactNullableString{
						length: int64(len(topicNames[0])),
						value:  topicNames[0],
					},
					topicID:    generateUUID(),
					partitions: []Partition{},
				},
			},
		}
	} else {
		responseBody = DescribeTopicPartitionsResponse{
			topics: relevantTopics,
		}
	}

	serializedResponse = responseBody.serialize()

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

func parseTopicNames(buf []byte) []string {
	bufReader := bytes.NewBuffer(buf)

	lenOfTopics, err := binary.ReadUvarint(bufReader)
	if err != nil {
		fmt.Errorf("Error parsing len of topics (in: parseTopicName): ", err)
	}

	names := []string{}
	for range lenOfTopics - 1 {
		lenOfName := int8(bufReader.Next(1)[0])

		name := string(bufReader.Next(int(lenOfName) - 1))
		names = append(names, name)

		// TAG_BUFFER
		_ = bufReader.Next(1)
	}

	return names
}

// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER
//
//	topics => name TAG_BUFFER
//	  name => COMPACT_STRING
//	response_partition_limit => INT32
//	cursor => topic_name partition_index TAG_BUFFER
//	  topic_name => COMPACT_STRING
//	  partition_index => INT32
type DescribeTopicPartitionsRequest struct {
	topics                 []Topic
	responsePartitionLimit int32
	cursor                 Cursor
	tagBuffer              int8
}

// DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER
//
//	throttle_time_ms => INT32
//	topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
//	  error_code => INT16
//	  name => COMPACT_NULLABLE_STRING
//	  topic_id => UUID
//	  is_internal => BOOLEAN
//	  partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER
//	    error_code => INT16
//	    partition_index => INT32
//	    leader_id => INT32
//	    leader_epoch => INT32
//	    replica_nodes => INT32
//	    isr_nodes => INT32
//	    eligible_leader_replicas => INT32
//	    last_known_elr => INT32
//	    offline_replicas => INT32
//	  topic_authorized_operations => INT32
//	next_cursor => topic_name partition_index TAG_BUFFER
//	  topic_name => COMPACT_STRING
//	  partition_index => INT32
type DescribeTopicPartitionsResponse struct {
	throttleTimeMS int32
	topics         []Topic
	nextCursor     Cursor
	tagBuffer      int8
}

type Topic struct {
	errorCode                 int16
	name                      CompactNullableString
	topicID                   UUID // TODO: change this to use "github.com/gofrs/uuid/v5" package
	isInternal                bool
	partitions                []Partition
	topicAuthorizedOperations int32
	tagBuffer                 int8
}

type Cursor struct {
	topicName      CompactString
	partitionIndex int32
	tagBuffer      int8
}

func (c Cursor) serialize() []byte {
	buf := new(bytes.Buffer)

	if c.topicName.value == "" && c.partitionIndex == 0 {
		binary.Write(buf, binary.BigEndian, uint8(255))
		return buf.Bytes()
	}

	binary.Write(buf, binary.BigEndian, c.topicName.serialize())
	binary.Write(buf, binary.BigEndian, c.partitionIndex)
	binary.Write(buf, binary.BigEndian, c.tagBuffer)

	return buf.Bytes()
}

type Partition struct {
	errorCode              int16
	partitionIndex         int32
	leaderId               int32
	leaderEpoch            int32
	replicaNodes           []int32
	isrNodes               []int32
	eligibleLeaderReplicas []int32
	lastKnownElr           []int32
	offlineReplicas        []int32
	tagBuffer              int8

	// The following fields are not part of the serialized response of this type
	topicID UUID
}

func (dtpr DescribeTopicPartitionsRequest) parse(buf []byte) {
	// lenOfTopics, n := binary.Varint(buf)
	// if n <= 0 {
	//     fmt.Println("Error decoding varint")
	//     return
	// }

	// TODO: This assumes there's only one topic in the array. Use above apprioach to determine actual length of array.

}

func (dtpr DescribeTopicPartitionsResponse) serialize() []byte {
	buf := new(bytes.Buffer)

	// Throttle time (ms)
	binary.Write(buf, binary.BigEndian, uint32(dtpr.throttleTimeMS))

	// Topic Length
	lenOfTopics := int8(len(dtpr.topics) + 1)
	binary.Write(buf, binary.BigEndian, lenOfTopics)

	for _, topic := range dtpr.topics {
		serializedTopic := topic.serialize()
		binary.Write(buf, binary.BigEndian, serializedTopic)
	}

	// Next Cursor
	binary.Write(buf, binary.BigEndian, dtpr.nextCursor.serialize())

	// TAG_BUFFER
	buf.Write([]byte{0})

	return buf.Bytes()
}

func (topic Topic) serialize() []byte {
	buf := new(bytes.Buffer)

	// ErrorCode
	binary.Write(buf, binary.BigEndian, topic.errorCode)

	// Topic name
	buf.Write(topic.name.serialize())

	// Topic ID
	binary.Write(buf, binary.BigEndian, topic.topicID.serialize())

	// isInternal
	buf.WriteByte(boolToByte(topic.isInternal))

	// Partition Length
	buf.Write([]byte{byte(len(topic.partitions) + 1)})

	// Partitions
	for _, partition := range topic.partitions {
		serializedPartition := partition.serialize()
		binary.Write(buf, binary.BigEndian, serializedPartition)
	}

	// Authorized Operations
	binary.Write(buf, binary.BigEndian, topic.topicAuthorizedOperations)

	// TAG_BUFFER
	buf.Write([]byte{0})

	return buf.Bytes()
}

func (partition Partition) serialize() []byte {
	buf := new(bytes.Buffer)

	// ErrorCode
	binary.Write(buf, binary.BigEndian, partition.errorCode)

	// Partition Index
	binary.Write(buf, binary.BigEndian, partition.partitionIndex)

	// Leader ID
	binary.Write(buf, binary.BigEndian, partition.leaderId)

	// Leader Epoch
	binary.Write(buf, binary.BigEndian, partition.leaderEpoch)

	// Replica nodes
	binary.Write(buf, binary.BigEndian, serializeArray(partition.replicaNodes))

	// ISR Nodes
	binary.Write(buf, binary.BigEndian, serializeArray(partition.isrNodes))

	// Eligible Replicas
	binary.Write(buf, binary.BigEndian, serializeArray(partition.eligibleLeaderReplicas))

	// Last Known ELR
	binary.Write(buf, binary.BigEndian, serializeArray(partition.lastKnownElr))

	// Offline Replicas
	binary.Write(buf, binary.BigEndian, serializeArray(partition.offlineReplicas))

	// TAG_BUFFER
	buf.Write([]byte{0})

	return buf.Bytes()
}

func serializeArray[T Integer](arr []T) []byte {
	buf := new(bytes.Buffer)

	// TODO: we're assuming len will be 1 byte. make this dynamic.
	lenOfArr := int8(len(arr) + 1)
	binary.Write(buf, binary.BigEndian, lenOfArr)

	for _, num := range arr {
		binary.Write(buf, binary.BigEndian, num)
	}

	return buf.Bytes()
}

// Define a constraint that includes all integer types
type Integer interface {
	constraints.Integer
}
