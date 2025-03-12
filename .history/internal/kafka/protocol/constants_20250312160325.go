// Package protocol provides implementations for the Kafka wire protocol
package protocol

// API Keys for Kafka protocol
const (
	ApiVersionsKey             int16 = 18
	DescribeTopicPartitionsKey int16 = 75
)

// Error codes for Kafka protocol
const (
	ErrorNone               uint16 = 0
	ErrorUnsupportedVersion uint16 = 35
	ErrorUnknownTopic       uint16 = 3
)

// Version constraints
const (
	MinSupportedVersion int16 = 0
	MaxSupportedVersion int16 = 4
)

// API version ranges
const (
	ApiVersionsMinVersion   int16 = 0
	ApiVersionsMaxVersion   int16 = 4
	DescribeTopicMinVersion int16 = 0
	DescribeTopicMaxVersion int16 = 0
)
