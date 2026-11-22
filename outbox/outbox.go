// Package outbox publishes messages to a message broker reliably, with the
// transactional outbox pattern.
//
// It takes three steps:
//
//   - [New] returns an [Outbox] for a [taskmill.Client].
//   - [Outbox.Store] saves each [Message] in the same transaction as the
//     business data that produced it, so a message exists if and only if
//     that data was committed.
//   - [Outbox.AddRelay] registers a relay on a worker. The relay publishes
//     the stored messages through a [Publisher], which wraps any broker
//     client: Kafka, NATS, RabbitMQ, or another.
//
// Messages with the same topic and key are published one at a time, in the
// order they were stored. A message that cannot be published is retried for
// about a month; then it is dead, and its key halts until an operator retries
// or deletes it, so later messages never overtake it.
//
// Publishing is at least once: a message can be published twice, for example
// if the worker crashes after the broker accepted it. Every delivery carries
// an ID that stays the same across retries, so consumers can deduplicate.
// The receiving side needs no package: a consumer that enqueues a task for
// each message, with the message ID as [taskmill.EnqueueOptions.DedupeKey],
// creates at most one task per message within the dedupe window.
//
// The [outbox guide] shows each step with a broker.
//
// [outbox guide]: https://github.com/code19m/taskmill/blob/main/docs/guides/outbox.md
package outbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/uuid"
)

// Queue is the taskmill queue that holds stored messages, as tasks of the
// kind "taskmill.outbox.message". A worker with a relay reads it by default.
// A published message is deleted right away: it is not kept in the task
// history.
const Queue = "outbox"

const (
	// maxAttempts with relayRetry keeps retrying a message for about a month.
	maxAttempts      = 10_000
	maxGroupKeyRunes = 256
)

// relayRetry retries a failed publish quickly at first, then every five
// minutes, so a broker outage of up to a month loses nothing.
var relayRetry = taskmill.ExponentialBackoff{Base: time.Second, Max: 5 * time.Minute}

// Message is an event to publish. Save it with [Outbox.Store].
type Message struct {
	// Topic is the broker destination, such as a Kafka topic. Required. It
	// must be valid UTF-8, without a NUL character, as must the names of
	// the headers. It must not hold the unit separator, U+001F, which
	// separates it from the key in the stream that orders its messages.
	Topic string

	// Key orders messages: messages with the same topic and key are
	// published one at a time, in the order they were stored. Messages
	// without a key are published in any order. Default: none.
	Key []byte

	// Value is the message body. The message is stored in one task payload,
	// as JSON: the key, the value, and the values of the headers
	// base64-encoded, and the topic and the names of the headers as text.
	// The payload's limit of 1 MiB allows about 750 KiB of Value. Default:
	// empty.
	Value []byte

	// Headers are passed to the broker with the message. They share the
	// size limit of Value. Default: none.
	Headers map[string][]byte
}

// Delivery is a stored message on its way to the broker.
type Delivery struct {
	Message

	// ID is unique across services and stays the same when a delivery is
	// retried, so consumers can deduplicate by it. It is a UUIDv7.
	ID string

	// Metadata is the metadata that [taskmill.EnqueueMiddleware] stored with
	// the message, such as the trace context of the transaction that stored
	// it.
	Metadata map[string]string
}

// Publisher sends deliveries to a broker. It returns one error per delivery,
// in the same order: nil marks that delivery as published, an error retries
// it later, and an error wrapped with [taskmill.Permanent] makes it dead
// right away. A batch holds at most [RelayOptions.BatchSize] deliveries. Of
// the deliveries that have a key, no two in a batch have the same topic and
// key. Deliveries without a key have no order to keep, so a batch can hold
// many of one topic. Each call has [RelayOptions.Timeout]: then its context
// is canceled, and the deliveries without a nil result are retried.
//
// A delivery without a result is retried, as after an error: Publish
// returned nil, or fewer errors than deliveries. Nil never means that all
// were published. So a publisher that ends in "return nil" after it sent
// its deliveries sends them all again at each retry, until they are dead.
// When all were published, return make([]error, len(deliveries)). More
// errors than deliveries retry every delivery: no error can be matched to
// its delivery, as when append added the errors after the nils that make
// put in the slice.
//
// Publish must return when ctx ends. A call that ignores it keeps running:
// 5 seconds after the timeout, its deliveries are retried while it still
// runs. A send that it makes late can then reach the broker after later
// messages of its key, and become the key's last record, which deduplication
// by ID does not undo. With a broker client that takes no context, give the
// client a send timeout below Timeout.
//
// A failed delivery is retried after 1 second, then after twice as long each
// time, up to every 5 minutes, with ±10% jitter. After 10,000 tries, about a
// month, the message is dead.
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

// New returns an outbox that keeps its messages in client's database. With a
// nil client, [Outbox.Store] returns an error that wraps
// [taskmill.ErrInvalidArgument].
func New(client *taskmill.Client) *Outbox {
	return &Outbox{client: client}
}

// Store saves msgs in the outbox inside the caller's transaction, so they are
// published only if the transaction commits. A message without a topic, with
// a topic or a header name that is not valid UTF-8 or holds a NUL character,
// with a topic that holds U+001F (see [Message.Topic]), or over the payload
// limit (see [Message.Value]), or a call over the limits
// of [taskmill.Client.EnqueueMany], 10,000 messages and 64 MiB, fails with an
// error that wraps [taskmill.ErrInvalidArgument], and leaves the transaction
// usable. A call with no messages does nothing.
func (o *Outbox) Store(ctx context.Context, tx taskmill.Executor, msgs ...Message) error {
	if o.client == nil {
		return fmt.Errorf("%w: outbox client is nil", taskmill.ErrInvalidArgument)
	}
	if len(msgs) == 0 {
		return nil
	}

	params := make([]taskmill.EnqueueParams, len(msgs))
	for i, m := range msgs {
		if problem := checkNames(m); problem != "" {
			return fmt.Errorf("%w: outbox message %d of %d %s", taskmill.ErrInvalidArgument, i+1, len(msgs), problem)
		}
		params[i] = taskmill.EnqueueParams{
			Args:    message{ID: uuid.NewV7(), Topic: m.Topic, Key: m.Key, Value: m.Value, Headers: m.Headers},
			Options: &taskmill.EnqueueOptions{GroupKey: groupKey(m.Topic, m.Key)},
		}
	}

	_, err := o.client.EnqueueManyTx(ctx, tx, params)
	return err
}

// checkNames returns what is wrong with the topic and the header names of m,
// or "". They are stored as JSON text, which would turn bytes that are not
// UTF-8 into U+FFFD: two topics would become one, while the keys that order
// their messages stay apart. And jsonb cannot hold a NUL character. A topic
// that held the separator of groupKey could share the group of another
// topic and key, so that a dead message of one halts the other.
func checkNames(m Message) string {
	switch {
	case m.Topic == "":
		return "has no topic"
	case !text(m.Topic):
		return "has a topic that is not valid UTF-8 or holds a NUL character"
	case strings.ContainsRune(m.Topic, groupSeparator):
		return "has a topic that holds the unit separator, U+001F"
	}
	for name := range m.Headers {
		if !text(name) {
			return "has a header name that is not valid UTF-8 or holds a NUL character"
		}
	}
	return ""
}

// text reports whether s is valid UTF-8 without a NUL character.
func text(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// RelayOptions configure a relay. The zero value keeps the defaults.
type RelayOptions struct {
	// BatchSize is the most deliveries that one call of the publisher
	// receives. It must not be negative. Default: 100. Maximum: 1,000.
	//
	// A worker makes up to one call per slot of the [Queue]'s
	// [taskmill.QueueOptions.Concurrency], 10 by default, and each message
	// in a call holds about 2.5 times its Value in memory. The messages of a
	// call come in one fetch, while their leases run: on a slow link to the
	// database, the worker fetches fewer at a time, and a message whose lease
	// ended on the way is not published, but retried. Lower BatchSize for
	// large messages, or a slow link.
	BatchSize int

	// Timeout bounds each call of the publisher: see [Publisher]. It must
	// not be negative. Default: 1 minute.
	Timeout time.Duration
}

// AddRelay registers the batch handler that publishes stored messages through
// pub. opts may be nil. A worker with no Queues set reads [Queue] for it.
// [taskmill.NewWorker] reports a nil pub, or options out of range, as an
// error that wraps [taskmill.ErrInvalidArgument].
func (*Outbox) AddRelay(hs *taskmill.Handlers, pub Publisher, opts *RelayOptions) {
	ho := &taskmill.HandlerOptions{Retry: relayRetry, DiscardCompleted: true}
	if opts != nil {
		ho.BatchSize, ho.Timeout = opts.BatchSize, opts.Timeout
	}

	if pub == nil {
		taskmill.AddBatchHandler[message](hs, nil, ho)
		return
	}
	taskmill.AddBatchHandler[message](hs, relay{pub: pub}, ho)
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

	// Clipped, so that the errors added here stay out of the publisher's
	// own slice.
	errs := slices.Clip(r.pub.Publish(ctx, ds))
	if len(errs) > len(ds) {
		err := fmt.Errorf("the publisher returned %d results for %d deliveries", len(errs), len(ds))
		errs = slices.Repeat([]error{err}, len(ds))
	}
	for len(errs) < len(ds) {
		errs = append(errs, errNoResult)
	}
	return errs
}

// errNoResult fails a delivery for which the publisher returned no result.
// It names the publisher, which is what the application wrote: the worker's
// error for a missing result names the batch handler, which is the relay.
var errNoResult = errors.New("the publisher returned no result for this delivery")

// groupSeparator joins a topic and a key in a group key. A topic cannot
// hold it, so the first one ends the topic, and no two pairs share a group.
const groupSeparator = '\x1f'

// groupKey orders the messages of one topic and key. The two are joined by
// groupSeparator. Keys that are long, binary, or contain a NUL byte, which a
// group key cannot hold, are hashed.
func groupKey(topic string, key []byte) string {
	if len(key) == 0 {
		return ""
	}
	g := topic + string(groupSeparator) + string(key)
	if !utf8.ValidString(g) || strings.ContainsRune(g, 0) || utf8.RuneCountInString(g) > maxGroupKeyRunes {
		sum := sha256.Sum256([]byte(g))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	return g
}
