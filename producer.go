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

	msg := `{"topic": "chat", "value": "Hello there!, I am John Doe"}`

	conn.Write([]byte(msg + "\n"))

	buffer := make([]byte, 1024)

	n, err := conn.Read(buffer)
	
	fmt.Println(string(buffer[:n]))
}