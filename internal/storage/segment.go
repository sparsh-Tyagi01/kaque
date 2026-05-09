package storage

import "os"

type Segment struct {
	BaseOffset int64
	NextOffset int64
	File *os.File
	Size int64
}

func NewSegment(path string, baseOffset int64) (*Segment, error)  {
	file, err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0644)

	if err != nil {
		file.Close()
		return nil, err
	}

	stat, _ := file.Stat()

	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		File: file,
		Size: stat.Size(),
	}, nil
}

func (s *Segment) Append(data []byte) error  {
	n, err := s.File.Write(
		append(data, '\n'),
	)

	if err != nil {
		return err
	}

	s.Size = int64(n)

	return nil
}