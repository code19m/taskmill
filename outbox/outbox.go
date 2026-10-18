// Package outbox publishes messages to a message broker reliably, with the
// transactional outbox pattern.
//
// [Outbox.Store] saves messages in the same transaction as the business data
// that produced them, so a message exists if and only if that data was
// committed. A relay, registered on a worker with [Outbox.AddRelay], then
// publishes the stored messages through a [Publisher] that wraps any broker
// client: Kafka, NATS, RabbitMQ, or another. Messages with the same topic and
// key are published one at a time, in the order they were stored.
//
// Publishing is at least once: a message can be published twice, for example
// if the worker crashes after the broker accepted it. Every delivery carries
// an ID that stays the same across retries, so consumers can deduplicate.
// The receiving side needs no package: a consumer that enqueues a task for
// each message, with the message ID as [taskmill.EnqueueOptions.DedupeKey],
// creates at most one task per message within the dedupe window.
package outbox

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/uuid"
)

// DefaultQueue is the queue that holds outbox messages unless [Options] says
// otherwise.
const DefaultQueue = "outbox"

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

	// Value is the message body.
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

// Options configure an [Outbox]. The zero value is valid.
type Options struct {
	// Queue is the taskmill queue that holds pending messages.
	// Default: [DefaultQueue].
	Queue string

	// BatchSize is the most deliveries one Publish call receives.
	// Default: 100.
	BatchSize int
}

// ErrNoTopic is returned by [Outbox.Store] for a message without a topic.
var ErrNoTopic = errors.New("outbox: message has no topic")

var errNoClient = fmt.Errorf("outbox: %w: client is nil", taskmill.ErrInvalidArgument)

// Outbox stores messages and relays them to a broker. Create one with [New]
// and share it: it is safe for concurrent use. Every service that stores or
// relays messages of one outbox must use the same options.
type Outbox struct {
	client    *taskmill.Client
	queue     string
	batchSize int
}

// New returns an outbox that keeps its messages in client's database. opts
// may be nil.
func New(client *taskmill.Client, opts *Options) *Outbox {
	o := &Outbox{client: client, queue: DefaultQueue}
	if opts != nil {
		o.queue = cmp.Or(opts.Queue, DefaultQueue)
		o.batchSize = opts.BatchSize
	}
	return o
}

// Queue returns the taskmill queue that holds the outbox's messages, for a
// worker's [taskmill.WorkerConfig.Queues].
func (o *Outbox) Queue() string {
	return o.queue
}

// Store saves msgs in the outbox inside the caller's transaction, so they are
// published only if the transaction commits. Run [Outbox.Setup] once per
// deploy: without it, a key whose message cannot be published does not halt,
// so later messages of that key could overtake it.
func (o *Outbox) Store(ctx context.Context, tx taskmill.Executor, msgs ...Message) error {
	if o.client == nil {
		return errNoClient
	}
	if len(msgs) == 0 {
		return nil
	}
	params := make([]taskmill.EnqueueParams, len(msgs))
	for i, m := range msgs {
		if m.Topic == "" {
			return fmt.Errorf("%w (message %d of %d)", ErrNoTopic, i+1, len(msgs))
		}
		params[i] = taskmill.EnqueueParams{
			Args: message{ID: uuid.NewV7(), Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers},
			Options: &taskmill.EnqueueOptions{
				Queue:       o.queue,
				GroupKey:    groupKey(m.Topic, m.Key),
				MaxAttempts: maxAttempts,
			},
		}
	}
	if _, err := o.client.EnqueueManyTx(ctx, tx, params); err != nil {
		return fmt.Errorf("outbox: store: %w", err)
	}
	return nil
}

// AddRelay registers the batch handler that publishes stored messages through
// pub. Serve [Outbox.Queue] with a worker to run it. A nil pub is reported by
// [taskmill.NewWorker].
func (o *Outbox) AddRelay(hs *taskmill.Handlers, pub Publisher) {
	opts := &taskmill.HandlerOptions{BatchSize: o.batchSize, Retry: relayRetry}
	if pub == nil {
		taskmill.AddBatchHandler[message](hs, nil, opts)
		return
	}
	taskmill.AddBatchHandler[message](hs, relay{pub: pub}, opts)
}

// Setup applies the settings that the outbox's queue needs. Run it once per
// deploy, for example next to [taskmill.Migrate]: a key whose message cannot
// be published halts until an operator retries or deletes that message, so
// later messages never overtake it, and published messages are not kept in
// history. The queue's concurrency limit, if an operator set one, is kept.
func (o *Outbox) Setup(ctx context.Context) error {
	if err := o.setup(ctx); err != nil {
		return fmt.Errorf("outbox: setup: %w", err)
	}
	return nil
}

func (o *Outbox) setup(ctx context.Context) error {
	if o.client == nil {
		return errNoClient
	}
	queues, err := o.client.Queues(ctx)
	if err != nil {
		return err
	}
	settings := taskmill.QueueSettings{HaltGroupOnDead: true, DiscardCompleted: true}
	for _, q := range queues {
		if q.Name == o.queue {
			settings.MaxRunning = q.Settings.MaxRunning
		}
	}
	return o.client.UpdateQueue(ctx, o.queue, &settings)
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
