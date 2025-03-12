package protocol

// Request represents a Kafka protocol request
type Request struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	Payload       []byte
}

// Response represents a generic Kafka protocol response
type Response struct {
	CorrelationID int32
	ErrorCode     uint16
	Payload       []byte
}
