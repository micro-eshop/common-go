package microeshop

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
)

type NatsMessageMetaData struct {
	Headers map[string]string
	Topic   string
}

func NewNatsMessageMetaData(topic string) NatsMessageMetaData {
	headers := make(map[string]string)
	return NatsMessageMetaData{
		Topic:   topic,
		Headers: headers,
	}
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
	Publish(context.Context, NatsMessage[T]) error
}

type messagePubliser[T any] struct {
	client *NatsClient
}

func (publisher messagePubliser[T]) Publish(ctx context.Context, msg NatsMessage[T]) error {
	json, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	message := nats.NewMsg(msg.MetaData.Topic)
	message.Data = json
	for key, value := range msg.MetaData.Headers {
		message.Header.Set(key, value)
	}
	return publisher.client.connection.PublishMsg(message)
}

func NewMessagePublisher[T any](client *NatsClient) MessagePublisher[T] {
	return messagePubliser[T]{client: client}
}
