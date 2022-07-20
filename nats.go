package microeshop

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type NatsMessageMetaData struct {
	Topic string
}

func NewNatsMessageMetaData(topic string) NatsMessageMetaData {
	return NatsMessageMetaData{
		Topic: topic,
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
	client        *NatsClient
	traceProvider trace.TracerProvider
}

func (publisher messagePubliser[T]) Publish(ctx context.Context, msg NatsMessage[T]) error {
	json, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	message := nats.NewMsg(msg.MetaData.Topic)
	span := publisher.startNatsSpam(ctx, msg.MetaData.Topic, message)
	defer span.End()
	message.Data = json
	err = publisher.client.connection.PublishMsg(message)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (client *messagePubliser[T]) startNatsSpam(ctx context.Context, topic string, msg *nats.Msg) trace.Span {
	carrier := NewNatsCarrier(msg)
	propagator := otel.GetTextMapPropagator()
	ctx = propagator.Extract(ctx, carrier)
	ctx, span := client.traceProvider.Tracer(defaultTracerName).Start(ctx, "publish")
	span.SetAttributes(
		attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(topic)},
		attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(topic)},
		attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("nats")},
		attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("topic")},
		attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("TCP")},
		attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(client.client.connection.ConnectedAddr())},
	)
	propagator.Inject(ctx, carrier)
	return span
}

func NewMessagePublisher[T any](client *NatsClient, traceProvider trace.TracerProvider) MessagePublisher[T] {
	return messagePubliser[T]{client: client, traceProvider: traceProvider}
}

type NatsCarrier struct {
	msg *nats.Msg
}

// NewConsumerMessageCarrier creates a new ConsumerMessageCarrier.
func NewNatsCarrier(msg *nats.Msg) NatsCarrier {
	setHeaderIfEmpty(msg)
	return NatsCarrier{msg: msg}
}

func setHeaderIfEmpty(msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
}

// Get retrieves a single value for a given key.
func (c NatsCarrier) Get(key string) string {
	return c.msg.Header.Get(key)
}

// Set sets a header.
func (c NatsCarrier) Set(key, val string) {
	c.msg.Header.Set(key, val)
}

func (c NatsCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Header))
	for k := range c.msg.Header {
		keys = append(keys, k)
	}
	return keys
}

const defaultTracerName = "nats"
