// Package outbox publishes messages to a message broker reliably, with the
// transactional outbox pattern.
//
// [Outbox.Store] saves messages in the same transaction as the business data
// that produced them, so a message exists if and only if that data was
// committed. A relay, registered on a worker with [Outbox.AddRelay], then
// publishes the stored messages through a [Publisher] that wraps any broker
// client: Kafka, NATS, RabbitMQ, or another. Messages with the same topic and
// key are published one at a time, in the order they were stored. A message
// that cannot be published is retried for about a month; then it is dead,
// and its key halts until an operator retries or deletes it, so later
// messages never overtake it.
//
// Publishing is at least once: a message can be published twice, for example
// if the worker crashes after the broker accepted it. Every delivery carries
// an ID that stays the same across retries, so consumers can deduplicate.
// The receiving side needs no package: a consumer that enqueues a task for
// each message, with the message ID as [taskmill.EnqueueOptions.DedupeKey],
// creates at most one task per message within the dedupe window.
package outbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/uuid"
)

// Queue is the taskmill queue that holds stored messages. A worker with a
// relay serves it by default.
const Queue = "outbox"

const (
	// maxAttempts with relayRetry keeps retrying a message for about a month.
	maxAttempts      = 10_000
	maxGroupKeyRunes = 256
)

// relayRetry retries a failed publish quickly at first, then every five
// minutes, so a broker outage of up to a month loses nothing.
var relayRetry = taskmill.ExponentialBackoff{Base: time.Second, Max: 5 * time.Minute}

// Message is an event to publish.
type Message struct {
	// Topic is the broker destination, such as a Kafka topic. Required.
	Topic string

	// Key orders messages: messages with the same topic and key are
	// published one at a time, in the order they were stored. Messages
	// without a key are published in any order.
	Key []byte

	// Value is the message body. It is stored base64-encoded in a task
	// payload, whose 1 MiB limit allows about 750 KiB of Value.
	Value []byte

	// Headers are passed to the broker with the message.
	Headers map[string][]byte
}

// Delivery is a stored message on its way to the broker.
type Delivery struct {
	Message

	// ID is unique across services and stays the same when a delivery is
	// retried, so consumers can deduplicate by it. It is a UUIDv7.
	ID string

	// Metadata is the metadata that taskmill stored with the message, from
	// [taskmill.ClientConfig.ContextMetadata] and enqueue middleware, such as
	// the trace context of the transaction that stored it.
	Metadata map[string]string
}

// Publisher sends deliveries to a broker. It returns one error per delivery,
// in the same order: nil marks that delivery as published, an error retries
// it later, and an error wrapped with [taskmill.Permanent] makes it dead at
// once. A batch holds at most 100 deliveries, never two with the same topic
// and key. Each call has 1 minute: then its context is canceled, and the
// deliveries without a nil result are retried.
type Publisher interface {
	Publish(ctx context.Context, deliveries []Delivery) []error
}

// PublisherFunc adapts a function to the [Publisher] interface.
type PublisherFunc func(ctx context.Context, deliveries []Delivery) []error

// Publish calls f(ctx, deliveries).
func (f PublisherFunc) Publish(ctx context.Context, deliveries []Delivery) []error {
	return f(ctx, deliveries)
}

// Outbox stores messages and relays them to a broker. Create one with [New]
// and share it: it is safe for concurrent use.
type Outbox struct {
	client *taskmill.Client
}

// New returns an outbox that keeps its messages in client's database.
func New(client *taskmill.Client) *Outbox {
	return &Outbox{client: client}
}

// Store saves msgs in the outbox inside the caller's transaction, so they are
// published only if the transaction commits. A message without a topic fails
// with an error that wraps [taskmill.ErrInvalidArgument].
func (o *Outbox) Store(ctx context.Context, tx taskmill.Executor, msgs ...Message) error {
	if o.client == nil {
		return fmt.Errorf("%w: outbox client is nil", taskmill.ErrInvalidArgument)
	}
	if len(msgs) == 0 {
		return nil
	}
	params := make([]taskmill.EnqueueParams, len(msgs))
	for i, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("%w: outbox message %d of %d has no topic", taskmill.ErrInvalidArgument, i+1, len(msgs))
		}
		params[i] = taskmill.EnqueueParams{
			Args:    message{ID: uuid.NewV7(), Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers},
			Options: &taskmill.EnqueueOptions{GroupKey: groupKey(m.Topic, m.Key)},
		}
	}
	_, err := o.client.EnqueueManyTx(ctx, tx, params)
	return err
}

// AddRelay registers the batch handler that publishes stored messages through
// pub. A worker with no Queues set serves [Queue] for it. A nil pub is
// reported by [taskmill.NewWorker].
func (*Outbox) AddRelay(hs *taskmill.Handlers, pub Publisher) {
	opts := &taskmill.HandlerOptions{Retry: relayRetry, DiscardCompleted: true}
	if pub == nil {
		taskmill.AddBatchHandler[message](hs, nil, opts)
		return
	}
	taskmill.AddBatchHandler[message](hs, relay{pub: pub}, opts)
}

// message is the stored form of a Message.
type message struct {
	ID      string            `json:"id"`
	Topic   string            `json:"topic"`
	Key     []byte            `json:"key,omitempty"`
	Value   []byte            `json:"value"`
	Headers map[string][]byte `json:"headers,omitempty"`
}

func (message) Kind() string { return "taskmill.outbox.message" }

func (message) EnqueueDefaults() taskmill.EnqueueOptions {
	return taskmill.EnqueueOptions{Queue: Queue, MaxAttempts: maxAttempts}
}

type relay struct {
	pub Publisher
}

func (r relay) HandleBatch(ctx context.Context, tasks []*taskmill.Task[message]) []error {
	ds := make([]Delivery, len(tasks))
	for i, t := range tasks {
		m := t.Args
		ds[i] = Delivery{
			ID:       m.ID,
			Message:  Message{Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers},
			Metadata: t.Metadata,
		}
	}
	return r.pub.Publish(ctx, ds)
}

// groupKey orders the messages of one topic and key. The two are joined by
// the ASCII unit separator. Keys that are long, binary, or contain a NUL
// byte, which a group key cannot hold, are hashed.
func groupKey(topic string, key []byte) string {
	if len(key) == 0 {
		return ""
	}
	g := topic + "\x1f" + string(key)
	if !utf8.ValidString(g) || strings.ContainsRune(g, 0) || utf8.RuneCountInString(g) > maxGroupKeyRunes {
		sum := sha256.Sum256([]byte(g))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return g
}
