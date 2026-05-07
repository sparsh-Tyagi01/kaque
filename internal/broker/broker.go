package broker

import (
	"fmt"

	"github.com/sparsh-Tyagi01/kaque/internal/topic"
)

type Broker struct {
	Topics map[string]*topic.Topic
}

func NewBroker() *Broker  {
	return &Broker{
		Topics: make(map[string]*topic.Topic),
	}
}

func (b *Broker) CreateTopic(name string, t *topic.Topic)  {
	b.Topics[name] = t
}

func (b *Broker) GetTopic(name string) (*topic.Topic, error) {
	t, ok := b.Topics[name]

	if !ok {
		return nil, fmt.Errorf("topic not found")
	}

	return t, nil
}