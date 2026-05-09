package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9092")

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	req := `{ "action": "Consume", "topic": "chat", "offset": 0, "partition": 0 }`

	conn.Write([]byte(req + "\n"))

	buffer := make([]byte, 4096)

	n, _ := conn.Read(buffer)

	fmt.Println(string(buffer[:n]))
}