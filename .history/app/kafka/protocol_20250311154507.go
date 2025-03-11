package kafka

import (
	"encoding/binary"
	"io"
)

// Kafka API Keys (partial list)
const (
	APIKeyProduce          int16 = 0
	APIKeyFetch            int16 = 1
	APIKeyListOffsets      int16 = 2
	APIKeyMetadata         int16 = 3
	APIKeyLeaderAndISR     int16 = 4
	APIKeyStopReplica      int16 = 5
	APIKeyAPIVersions      int16 = 18
	APIKeySASLHandshake    int16 = 17
	APIKeySASLAuthenticate int16 = 36
)

// Error codes
const (
	ErrorCodeNone                 int16 = 0
	ErrorCodeUnknown              int16 = -1
	ErrorCodeOffsetOutOfRange     int16 = 1
	ErrorCodeCorruptMessage       int16 = 2
	ErrorCodeUnsupportedVersion   int16 = 35
	ErrorCodeNotController        int16 = 41
	ErrorCodeUnsupportedForMsgVer int16 = 83
)

// RequestHeader represents the header of a Kafka request
type RequestHeader struct {
	Size          int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader represents the header of a Kafka response
type ResponseHeader struct {
	CorrelationID int32
	ErrorCode     int16
}

// APIVersionsResponse represents the response to an API versions request
type APIVersionsResponse struct {
	Header       ResponseHeader
	APIKeys      []APIKey
	ThrottleTime int32
}

// APIKey represents a supported API key with min and max versions
type APIKey struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// ReadRequestHeader reads a Kafka request header from a reader
func ReadRequestHeader(reader io.Reader) (*RequestHeader, error) {
	header := &RequestHeader{}
	if err := binary.Read(reader, binary.BigEndian, &header.Size); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.APIKey); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.APIVersion); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &header.CorrelationID); err != nil {
		return nil, err
	}

	// For now, we're skipping the ClientID reading as it's a variable-length string
	// In a production implementation, we would read the string length and then the string bytes

	// Skip remaining bytes in header
	tmp := make([]byte, header.Size-8)
	if err := binary.Read(reader, binary.BigEndian, &tmp); err != nil {
		return nil, err
	}

	return header, nil
}

// WriteAPIVersionsResponse writes an API versions response to a writer
func WriteAPIVersionsResponse(writer io.Writer, response *APIVersionsResponse) error {
	// Create the response buffer
	// For API versions v3-v4, the format is:
	// - CorrelationID (4 bytes)
	// - ErrorCode (2 bytes)
	// - API Key Count (2 bytes, not 1 byte as before)
	// - For each API Key:
	//   - API Key (2 bytes)
	//   - Min Version (2 bytes)
	//   - Max Version (2 bytes)
	//   - No tagged fields for v3
	// - Throttle Time (4 bytes)
	// - No tagged fields for v3

	// Calculate response size for the length field
	apiKeyCount := len(response.APIKeys)
	responseSize := 4 + 2 + 2 + (apiKeyCount * 6) + 4

	// Write the size of the response
	if err := binary.Write(writer, binary.BigEndian, int32(responseSize)); err != nil {
		return err
	}

	// Write correlation ID
	if err := binary.Write(writer, binary.BigEndian, response.Header.CorrelationID); err != nil {
		return err
	}

	// Write error code
	if err := binary.Write(writer, binary.BigEndian, response.Header.ErrorCode); err != nil {
		return err
	}

	// Write API key count (2 bytes, not 1 byte as before)
	if err := binary.Write(writer, binary.BigEndian, int16(apiKeyCount)); err != nil {
		return err
	}

	// Write each API key
	for _, apiKey := range response.APIKeys {
		if err := binary.Write(writer, binary.BigEndian, apiKey.APIKey); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, apiKey.MinVersion); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, apiKey.MaxVersion); err != nil {
			return err
		}
		// Note: No tagged fields for v3
	}

	// Write throttle time
	if err := binary.Write(writer, binary.BigEndian, response.ThrottleTime); err != nil {
		return err
	}

	// Note: No tagged fields for v3

	return nil
}

// WriteErrorResponse writes an error response to a writer
func WriteErrorResponse(writer io.Writer, correlationID int32, errorCode int16) error {
	// Write size (6 bytes: 4 for correlationID, 2 for errorCode)
	if err := binary.Write(writer, binary.BigEndian, int32(6)); err != nil {
		return err
	}

	// Write correlation ID
	if err := binary.Write(writer, binary.BigEndian, correlationID); err != nil {
		return err
	}

	// Write error code
	if err := binary.Write(writer, binary.BigEndian, errorCode); err != nil {
		return err
	}

	return nil
}
