// Package outbox publishes messages to a message broker reliably, with the
// transactional outbox pattern.
//
// [Store] saves messages in the same transaction as the business data that
// produced them, so a message exists if and only if that data was committed.
// A relay, registered on a worker with [AddRelay], then publishes the stored
// messages through a [Publisher] that wraps any broker client: Kafka, NATS,
// RabbitMQ, or another. Messages with the same topic and key are published
// one at a time, in the order they were stored.
//
// Publishing is at least once: a message can be published twice, for example
// if the worker crashes after the broker accepted it. Every delivery carries
// an ID that stays the same across retries, so consumers can deduplicate.
// The receiving side needs no package: a consumer that enqueues a task for
// each message, with the message ID as [taskmill.EnqueueOptions.DedupeKey],
// handles every message once.
package outbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/uuid"
)

// DefaultQueue is the queue that holds outbox messages unless [Options] says
// otherwise.
const DefaultQueue = "outbox"

const (
	maxAttempts      = 100
	maxGroupKeyRunes = 256
)

// relayRetry retries a failed publish quickly at first, then every five
// minutes, so a broker outage of a few days loses nothing.
var relayRetry = taskmill.ExponentialBackoff{Base: time.Second, Max: 5 * time.Minute}

// Message is an event to publish.
type Message struct {
	// Topic is the broker destination, such as a Kafka topic. Required.
	Topic string

	// Key orders messages: messages with the same topic and key are
	// published one at a time, in the order they were stored. Messages
	// without a key are published in any order.
	Key string

	// Value is the message body.
	Value []byte

	// Headers are passed to the broker with the message.
	Headers map[string]string
}

// Delivery is a stored message on its way to the broker.
type Delivery struct {
	Message

	// ID is unique across services and stays the same when a delivery is
	// retried, so consumers can deduplicate by it. It is a UUIDv7.
	ID string
}

// Publisher sends deliveries to a broker. It returns one error per delivery,
// in the same order: nil marks that delivery as published, and an error
// retries it later. A batch never holds two deliveries with the same topic
// and key.
type Publisher interface {
	Publish(ctx context.Context, deliveries []Delivery) []error
}

// PublisherFunc adapts a function to the [Publisher] interface.
type PublisherFunc func(ctx context.Context, deliveries []Delivery) []error

// Publish calls f(ctx, deliveries).
func (f PublisherFunc) Publish(ctx context.Context, deliveries []Delivery) []error {
	return f(ctx, deliveries)
}

// Options configure the outbox. The zero value is valid; the same options
// must be used to store, relay, and set up.
type Options struct {
	// Queue is the taskmill queue that holds pending messages.
	// Default: [DefaultQueue].
	Queue string

	// BatchSize is the most deliveries one Publish call receives.
	// Default: 100.
	BatchSize int
}

func (o *Options) queue() string {
	if o == nil || o.Queue == "" {
		return DefaultQueue
	}
	return o.Queue
}

// ErrNoTopic is returned by [Store] for a message without a topic.
var ErrNoTopic = errors.New("outbox: message has no topic")

// Store saves msgs in the outbox inside the caller's transaction, so they are
// published only if the transaction commits. opts may be nil.
func Store(ctx context.Context, client *taskmill.Client, tx taskmill.Executor, opts *Options, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	queue := opts.queue()
	params := make([]taskmill.EnqueueParams, len(msgs))
	for i, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("%w (message %d of %d)", ErrNoTopic, i+1, len(msgs))
		}
		params[i] = taskmill.EnqueueParams{
			Args: message{ID: uuid.NewV7(), Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers},
			Options: &taskmill.EnqueueOptions{
				Queue:       queue,
				GroupKey:    groupKey(m.Topic, m.Key),
				MaxAttempts: maxAttempts,
			},
		}
	}
	if _, err := client.EnqueueManyTx(ctx, tx, params); err != nil {
		return fmt.Errorf("outbox: store: %w", err)
	}
	return nil
}

// AddRelay registers the batch handler that publishes stored messages through
// pub. Serve the outbox queue with a worker to run it. opts may be nil.
func AddRelay(hs *taskmill.Handlers, pub Publisher, opts *Options) {
	size := 0
	if opts != nil {
		size = opts.BatchSize
	}
	taskmill.AddBatchHandler[message](hs, relay{pub: pub}, &taskmill.HandlerOptions{
		BatchSize: size,
		Retry:     relayRetry,
	})
}

// Setup applies the settings the outbox queue needs, once per installation:
// a key whose message cannot be published halts until an operator retries or
// deletes that message, so later messages never overtake it, and published
// messages are not kept in history. opts may be nil.
func Setup(ctx context.Context, client *taskmill.Client, opts *Options) error {
	err := client.UpdateQueue(ctx, opts.queue(), &taskmill.QueueSettings{
		HaltGroupOnDead:  true,
		DiscardCompleted: true,
	})
	if err != nil {
		return fmt.Errorf("outbox: setup: %w", err)
	}
	return nil
}

// message is the stored form of a Message.
type message struct {
	ID      string            `json:"id"`
	Topic   string            `json:"topic"`
	Key     string            `json:"key,omitempty"`
	Value   []byte            `json:"value"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (message) Kind() string { return "taskmill.outbox.message" }

type relay struct {
	pub Publisher
}

func (r relay) HandleBatch(ctx context.Context, tasks []*taskmill.Task[message]) []error {
	ds := make([]Delivery, len(tasks))
	for i, t := range tasks {
		m := t.Args
		ds[i] = Delivery{ID: m.ID, Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers}
	}
	return r.pub.Publish(ctx, ds)
}

// groupKey orders the messages of one topic and key. The two are joined by
// the ASCII unit separator, which topics and keys do not contain, and long
// keys are hashed to fit the group key limit.
func groupKey(topic, key string) string {
	if key == "" {
		return ""
	}
	g := topic + "\x1f" + key
	if utf8.RuneCountInString(g) > maxGroupKeyRunes {
		sum := sha256.Sum256([]byte(g))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return g
}
