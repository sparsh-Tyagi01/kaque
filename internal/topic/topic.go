package topic

import "github.com/sparsh-Tyagi01/kaque/internal/partition"

type Topic struct {
	Name string
	Partitions []*partition.Partition
}