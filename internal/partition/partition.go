package partition

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/sparsh-Tyagi01/kaque/internal/protocol"
)

type Partition struct {
	ID     int
	File   *os.File
	Offset int64
	Mutex  *sync.Mutex
}

func NewPartition(id int, path string) (*Partition, error) {
	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		return nil, err
	}

	// Continue offsets after restart by counting existing JSON lines.
	// (Each appended message writes exactly one line.)
	offset, err := countLines(path)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Partition{
		ID:     id,
		File:   file,
		Offset: offset,
		Mutex:  &sync.Mutex{},
	}, nil
}

func countLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var lines int64
	buf := make([]byte, 32*1024)
	var leftover []byte

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := append(leftover, buf[:n]...)
			for i := 0; i < len(chunk); i++ {
				if chunk[i] == '\n' {
					lines++
				}
			}
			// Keep trailing bytes after the last newline (if any)
			lastNL := -1
			for i := len(chunk) - 1; i >= 0; i-- {
				if chunk[i] == '\n' {
					lastNL = i
					break
				}
			}
			if lastNL == -1 {
				leftover = chunk
			} else {
				leftover = chunk[lastNL+1:]
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
	}

	return lines, nil
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

func (p *Partition) Read(offset int64) ([]protocol.Message, error)  {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	file, err := os.Open(p.File.Name())

	if err != nil {
		return nil, err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	var messages []protocol.Message

	for scanner.Scan() {
		line := scanner.Bytes()

		var msg protocol.Message

		err := json.Unmarshal(line, &msg)

		if err != nil {
			continue
		}

		if msg.Offset >= offset {
			messages = append(messages, msg)
		}
	}

	return messages, nil
}
