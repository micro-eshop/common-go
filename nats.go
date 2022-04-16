package microeshop

type NatsMessageMetaData struct {
	Headers map[string]any
}

type NatsMessage[T any] struct {
	Data     T
	MetaData NatsMessageMetaData
}
