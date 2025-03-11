package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
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
		var correlationId, length int32
		var requestApiKey, requestApiVersion int16

		// messageSize := 0
		binary.Read(conn, binary.BigEndian, &length)
		binary.Read(conn, binary.BigEndian, &requestApiKey)
		binary.Read(conn, binary.BigEndian, &requestApiVersion)
		binary.Read(conn, binary.BigEndian, &correlationId)
		fmt.Println(length)
		fmt.Println(requestApiKey)
		fmt.Println(requestApiVersion)
		fmt.Println(correlationId)

		tmp := make([]byte, length-8)
		binary.Read(conn, binary.BigEndian, &tmp)
		// // messageSize := buffer[0:4]
		// requestApiKey := buffer[4:6]
		// requestApiVersion := buffer[6:8]
		// correlationId := binary.BigEndian.Uint32(buffer[8:12])
		// // tagBuffer := buffer[12:]
		// intRequestApiVersion := int16(binary.BigEndian.Uint16(requestApiVersion))

		if requestApiVersion < 0 || requestApiVersion > 4 {
			out := make([]byte, 6)
			binary.BigEndian.PutUint32(out, uint32(correlationId))
			binary.BigEndian.PutUint16(out[4:], uint16(35))
			send(conn, out)
			os.Exit(1)
		}
		// errorSlice := make([]byte, 2)

		// // Convert int16 to []byte (big-endian)
		// binary.BigEndian.PutUint16(errorSlice, uint16(error))

		out := make([]byte, 25)
		binary.BigEndian.PutUint32(out, uint32(correlationId))
		binary.BigEndian.PutUint16(out[4:], 0)   // Error code
		out[6] = 2                               // Number of API keys
		binary.BigEndian.PutUint16(out[7:], 18)  // API Key 1 - API_VERSIONS
		binary.BigEndian.PutUint16(out[9:], 3)   //             min version
		binary.BigEndian.PutUint16(out[11:], 4)  //             max version
		binary.BigEndian.PutUint16(out[13:], 75) //
		binary.BigEndian.PutUint16(out[15:], 0)  //             min version
		binary.BigEndian.PutUint16(out[17:], 0)  //             max version
		out[19] = 0                              // _tagged_fields
		binary.BigEndian.PutUint32(out[20:], 0)  // throttle time
		out[24] = 0

		send(conn, out)
	}
}

func send(conn net.Conn, out []byte) {
	binary.Write(conn, binary.BigEndian, int32(len(out)))
	binary.Write(conn, binary.BigEndian, out)
}
