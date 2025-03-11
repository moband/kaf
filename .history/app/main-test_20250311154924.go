package test_main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func RunTestMain() {
	fmt.Println("Logs from your program will appear here!")

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
		go handleTestConnection(conn)
	}
}

func handleTestConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var correlationId, length int32
		var requestApiKey, requestApiVersion int16

		err := binary.Read(conn, binary.BigEndian, &length)
		if err != nil {
			return
		}
		err = binary.Read(conn, binary.BigEndian, &requestApiKey)
		if err != nil {
			return
		}
		err = binary.Read(conn, binary.BigEndian, &requestApiVersion)
		if err != nil {
			return
		}
		err = binary.Read(conn, binary.BigEndian, &correlationId)
		if err != nil {
			return
		}
		fmt.Println(length)
		fmt.Println(requestApiKey)
		fmt.Println(requestApiVersion)
		fmt.Println(correlationId)

		tmp := make([]byte, length-8)
		err = binary.Read(conn, binary.BigEndian, &tmp)
		if err != nil {
			return
		}

		if requestApiVersion < 0 || requestApiVersion > 4 {
			out := make([]byte, 6)
			binary.BigEndian.PutUint32(out, uint32(correlationId))
			binary.BigEndian.PutUint16(out[4:], uint16(35))
			sendTestResponse(conn, out)
			return
		}

		out := make([]byte, 26)
		binary.BigEndian.PutUint32(out, uint32(correlationId))
		binary.BigEndian.PutUint16(out[4:], 0)   // Error code
		out[6] = 3                               // Number of API keys
		binary.BigEndian.PutUint16(out[7:], 18)  // API Key 1 - API_VERSIONS
		binary.BigEndian.PutUint16(out[9:], 3)   //             min version
		binary.BigEndian.PutUint16(out[11:], 4)  //             max version
		out[13] = 0                              // _tagged_fields
		binary.BigEndian.PutUint16(out[14:], 75) //
		binary.BigEndian.PutUint16(out[16:], 0)  //             min version
		binary.BigEndian.PutUint16(out[18:], 0)  //             max version
		out[20] = 0                              // _tagged_fields
		binary.BigEndian.PutUint32(out[21:], 0)  // throttle time
		out[25] = 0

		sendTestResponse(conn, out)
	}
}

func sendTestResponse(conn net.Conn, out []byte) {
	binary.Write(conn, binary.BigEndian, int32(len(out)))
	binary.Write(conn, binary.BigEndian, out)
}
