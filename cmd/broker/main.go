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

	p0, _ := partition.NewPartition(0, "data/chat-0.log")
	p1, _ := partition.NewPartition(1, "data/chat-1.log")
	p2, _ := partition.NewPartition(2, "data/chat-2.log")

	t := &topic.Topic{
		Name: "chat",
		Partitions: []*partition.Partition{p0,p1,p2},
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

		var req protocol.Request

		err := json.Unmarshal([]byte(text), &req)

		if err != nil {
			fmt.Println(err)
			continue
		}

		t, err := b.GetTopic(req.Topic)

		if err != nil {
			continue
		}

		partitionIndex := broker.Hash(req.Value) % len(t.Partitions)

		if req.Action == "Produce" {
			msg := protocol.Message{
				Value: []byte(req.Value),
				Time: time.Now(),
			}

			msg.Partition = t.Partitions[partitionIndex].ID
			err = t.Partitions[partitionIndex].Append(msg)

			if err != nil {
				fmt.Println(err)
				continue
			}

			conn.Write([]byte("STORED\n"))
		}

		if req.Action == "Consume" {
			msgs, err := t.Partitions[req.Partition].Read(req.Offset)

			if err != nil {
				continue
			}

			data, _ := json.Marshal(msgs)

			conn.Write(append(data, '\n'))
		}
	}
}