package microeshop

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

type NatsMessageMetaData struct {
	Headers map[string]any
	Topic   string
}

type NatsMessage[T any] struct {
	Data     T
	MetaData NatsMessageMetaData
}

func NewNatsMessage[T any](data T) NatsMessage[T] {
	return NatsMessage[T]{
		Data:     data,
		MetaData: NatsMessageMetaData{},
	}
}

type NatsClient struct {
	connection *nats.Conn
}

func NewNatsClient(url string) (*NatsClient, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	return &NatsClient{connection: nc}, nil
}

func (c NatsClient) Close() {
	c.connection.Drain()
	c.connection.Close()
}

type MessagePublisher[T any] interface {
	Publish(msg NatsMessage[T]) error
}

type messagePubliser[T any] struct {
	client *NatsClient
}

func (publisher messagePubliser[T]) Publish(msg NatsMessage[T]) error {
	json, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return publisher.client.connection.Publish(msg.MetaData.Topic, json)
}

func NewMessagePublisher[T any](client *NatsClient) MessagePublisher[T] {
	return messagePubliser[T]{client: client}
}
