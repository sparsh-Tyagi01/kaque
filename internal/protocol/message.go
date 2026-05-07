package protocol

import "time"

type Message struct {
	Offset int64 `json:"offset"`
	Key []byte `json:"key"`
	Value []byte `json:"value"`
	Time time.Time `json:"time"`
}