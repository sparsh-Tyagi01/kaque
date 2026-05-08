package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/sparsh-Tyagi01/kaque/internal/broker"
	"github.com/sparsh-Tyagi01/kaque/internal/partition"
	"github.com/sparsh-Tyagi01/kaque/internal/protocol"
	"github.com/sparsh-Tyagi01/kaque/internal/topic"
)

func main() {
	b := broker.NewBroker()

	p, _ := partition.NewPartition(0, "data/chat.log")

	t := &topic.Topic{
		Name: "chat",
		Partition: []*partition.Partition{p},
	}

	b.CreateTopic("chat", t)

	ln, err := net.Listen("tcp", ":9092")

	if err != nil {
		panic(err)
	}

	fmt.Println("Broker running on PORT: 9092")

	for {
		conn, err := ln.Accept()

		if err != nil {
			continue
		}

		go handleConnection(conn, b)
	}
}

func handleConnection(conn net.Conn, b *broker.Broker)  {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		text := scanner.Text()

		var input struct {
			Topic string `json:"topic"`
			Value string `json:"value"`
		}

		err := json.Unmarshal([]byte(text), &input)

		if err != nil {
			fmt.Println(err)
			continue
		}

		t, err := b.GetTopic(input.Topic)

		if err != nil {
			continue
		}

		msg := protocol.Message{
			Value: []byte(input.Value),
			Time: time.Now(),
		}

		err = t.Partition[0].Append(msg)

		if err != nil {
			fmt.Println(err)
			continue
		}

		conn.Write([]byte("OK\n"))
	}
}