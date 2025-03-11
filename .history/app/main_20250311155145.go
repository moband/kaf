package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	// Accept connections in a loop
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read message size (4 bytes)
		var length int32
		err := binary.Read(conn, binary.BigEndian, &length)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading message size: ", err.Error())
			}
			return
		}

		// Read API key (2 bytes)
		var requestApiKey int16
		err = binary.Read(conn, binary.BigEndian, &requestApiKey)
		if err != nil {
			fmt.Println("Error reading API key: ", err.Error())
			return
		}

		// Read API version (2 bytes)
		var requestApiVersion int16
		err = binary.Read(conn, binary.BigEndian, &requestApiVersion)
		if err != nil {
			fmt.Println("Error reading API version: ", err.Error())
			return
		}

		// Read correlation ID (4 bytes)
		var correlationId int32
		err = binary.Read(conn, binary.BigEndian, &correlationId)
		if err != nil {
			fmt.Println("Error reading correlation ID: ", err.Error())
			return
		}

		// Debug logging
		fmt.Println("Message length:", length)
		fmt.Println("API Key:", requestApiKey)
		fmt.Println("API Version:", requestApiVersion)
		fmt.Println("Correlation ID:", correlationId)

		// Read the rest of the request (client ID, etc.)
		tmp := make([]byte, length-8)
		err = binary.Read(conn, binary.BigEndian, &tmp)
		if err != nil {
			fmt.Println("Error reading request body: ", err.Error())
			return
		}

		// Check API version - if unsupported, send error response
		if requestApiVersion < 0 || requestApiVersion > 4 {
			out := make([]byte, 6)
			binary.BigEndian.PutUint32(out, uint32(correlationId))
			binary.BigEndian.PutUint16(out[4:], uint16(35)) // UNSUPPORTED_VERSION
			send(conn, out)
			return
		}

		// If API key is 18 (API_VERSIONS), respond with supported API versions
		if requestApiKey == 18 {
			out := make([]byte, 26)
			binary.BigEndian.PutUint32(out, uint32(correlationId))
			binary.BigEndian.PutUint16(out[4:], 0)   // Error code
			out[6] = 3                               // Number of API keys
			binary.BigEndian.PutUint16(out[7:], 18)  // API Key 1 - API_VERSIONS
			binary.BigEndian.PutUint16(out[9:], 3)   //             min version
			binary.BigEndian.PutUint16(out[11:], 4)  //             max version
			out[13] = 0                              // _tagged_fields
			binary.BigEndian.PutUint16(out[14:], 75) // API Key 2
			binary.BigEndian.PutUint16(out[16:], 0)  //             min version
			binary.BigEndian.PutUint16(out[18:], 0)  //             max version
			out[20] = 0                              // _tagged_fields
			binary.BigEndian.PutUint32(out[21:], 0)  // throttle time
			out[25] = 0                              // _tagged_fields

			send(conn, out)
		} else {
			// Simple response for other API keys
			response := make([]byte, 10)

			// First 4 bytes: message_size
			binary.BigEndian.PutUint32(response[0:4], 6)

			// Next 4 bytes: correlation_id
			binary.BigEndian.PutUint32(response[4:8], uint32(correlationId))

			// Next 2 bytes: error_code (0 = no error)
			binary.BigEndian.PutUint16(response[8:10], 0)

			_, err = conn.Write(response)
			if err != nil {
				fmt.Println("Error sending response: ", err.Error())
				return
			}
		}
	}
}

func send(conn net.Conn, out []byte) {
	// Write message size followed by the actual message
	binary.Write(conn, binary.BigEndian, int32(len(out)))
	binary.Write(conn, binary.BigEndian, out)
}
