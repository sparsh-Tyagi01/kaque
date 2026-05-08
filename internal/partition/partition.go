package partition

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/sparsh-Tyagi01/kaque/internal/protocol"
)

type Partition struct {
	ID int
	File *os.File
	Offset int64
	Mutex *sync.Mutex
}

func NewPartition(id int, path string) (*Partition, error)  {
	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &Partition{
		ID: id,
		File: file,
		Offset: 0,
		Mutex: &sync.Mutex{},
	}, nil
}

func (p *Partition) Append(msg protocol.Message) error {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	msg.Offset = p.Offset

	data, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	_, err = p.File.Write(
		append(data, '\n'),
	)
	
	if err != nil {
		return err
	}

	p.Offset++

	return nil
}