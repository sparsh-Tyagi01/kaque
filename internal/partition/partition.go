package partition

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/sparsh-Tyagi01/kaque/internal/protocol"
	"github.com/sparsh-Tyagi01/kaque/internal/storage"
)

type Partition struct {
    ID int
    Segments []*storage.Segment
    Active   *storage.Segment
    Offset int64
    Mutex sync.Mutex
}

func NewPartition(id int, path string) (*Partition, error) {

	segment, err := storage.NewSegment(
		path,
		0,
	)

	if err != nil {
		return nil, err
	}

	// offset, err := countLines(path)

	return &Partition{
		ID:       id,
		Segments: []*storage.Segment{segment},
		Active:   segment,
		Offset:   0,
	}, nil
}

// func countLines(path string) (int64, error) {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer f.Close()

// 	var lines int64
// 	buf := make([]byte, 32*1024)
// 	var leftover []byte

// 	for {
// 		n, readErr := f.Read(buf)
// 		if n > 0 {
// 			chunk := append(leftover, buf[:n]...)
// 			for i := 0; i < len(chunk); i++ {
// 				if chunk[i] == '\n' {
// 					lines++
// 				}
// 			}

// 			lastNL := -1
// 			for i := len(chunk) - 1; i >= 0; i-- {
// 				if chunk[i] == '\n' {
// 					lastNL = i
// 					break
// 				}
// 			}
// 			if lastNL == -1 {
// 				leftover = chunk
// 			} else {
// 				leftover = chunk[lastNL+1:]
// 			}
// 		}
// 		if readErr == io.EOF {
// 			break
// 		}
// 		if readErr != nil {
// 			return 0, readErr
// 		}
// 	}

// 	return lines, nil
// }

func (p *Partition) Append(msg protocol.Message) error {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	msg.Offset = p.Offset

	const MaxSegmentSize = 1024 * 1024

	if p.Active.Size >= MaxSegmentSize {
		err := p.RotateSegment()

		if err != nil {
			return err
		}
	}

	data, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	err = p.Active.Append(data)

	if err != nil {
		return err
	}

	p.Active.NextOffset = p.Offset + 1

	return nil
}

func (p *Partition) Read(offset int64) ([]protocol.Message, error)  {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	var messages []protocol.Message

	for _, segment := range p.Segments {
		if segment.NextOffset <= offset {
			continue
		}

		msgs, err := p.readSegment(segment, offset)

		if err != nil {
			return nil, err
		}

		messages = append(messages, msgs...)
	}

	return messages, nil
}

func (p *Partition) RotateSegment() error {
    baseOffset := p.Offset

    path := fmt.Sprintf(
        "data/chat-%d-%d.log",
        p.ID,
        baseOffset,
    )

    segment, err := storage.NewSegment(
        path,
        baseOffset,
    )

    if err != nil {
        return err
    }

    p.Segments = append(
        p.Segments,
        segment,
    )

    p.Active = segment

    return nil
}

func (p *Partition) Cleanup() {
    if len(p.Segments) <= 1 {
        return
    }

    old := p.Segments[0]

    old.File.Close()

    os.Remove(old.File.Name())

    p.Segments = p.Segments[1:]
}

func (p *Partition) readSegment(
    segment *storage.Segment,
    offset int64,
) ([]protocol.Message, error) {

    file, err := os.Open(
        segment.File.Name(),
    )

    if err != nil {
        return nil, err
    }

    defer file.Close()

    scanner := bufio.NewScanner(file)

    var messages []protocol.Message

    for scanner.Scan() {

        line := scanner.Bytes()

        var msg protocol.Message

        err := json.Unmarshal(
            line,
            &msg,
        )

        if err != nil {
            continue
        }

        if msg.Offset >= offset {
            messages = append(
                messages,
                msg,
            )
        }
    }

    return messages, nil
}