package protocol

import "time"

type Message struct {
	Offset int64 `json:"offset"`
	Partition int `json:"partition"`
	Key []byte `json:"key"`
	Value []byte `json:"value"`
	Time time.Time `json:"time"`
}