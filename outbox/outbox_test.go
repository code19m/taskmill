package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/code19m/taskmill"
	"github.com/code19m/taskmill/internal/testdb"
	"github.com/code19m/taskmill/outbox"
)

func TestMain(m *testing.M) {
	outbox.FastRetry()
	os.Exit(m.Run())
}

func setup(t *testing.T) (*taskmill.Client, *outbox.Outbox, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.Pool(t)
	schema := testdb.Schema(t, pool)
	if err := taskmill.Migrate(t.Context(), pool, &taskmill.MigrateConfig{Schema: schema}); err != nil {
		t.Fatal(err)
	}
	client, err := taskmill.NewClient(t.Context(), pool, &taskmill.ClientConfig{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	ob := outbox.New(client, &outbox.Options{BatchSize: 16})
	if err := ob.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return client, ob, pool
}

// broker records what it publishes and fails each delivery's first attempt
// when asked to.
type broker struct {
	mu        sync.Mutex
	published map[string][]string // per "topic/key": values in publish order
	ids       map[string]string   // value -> delivery ID
	failed    map[string]bool
	failFirst bool
}

func newBroker(failFirst bool) *broker {
	return &broker{published: map[string][]string{}, ids: map[string]string{}, failed: map[string]bool{}, failFirst: failFirst}
}

func (b *broker) Publish(_ context.Context, ds []outbox.Delivery) []error {
	b.mu.Lock()
	defer b.mu.Unlock()
	errs := make([]error, len(ds))
	seen := map[string]bool{}
	for i, d := range ds {
		stream := d.Topic + "/" + string(d.Key)
		if len(d.Key) > 0 && seen[stream] {
			errs[i] = errors.New("two deliveries of one key in a batch")
			continue
		}
		seen[stream] = true
		v := string(d.Value)
		if prev, ok := b.ids[v]; ok && prev != d.ID {
			errs[i] = fmt.Errorf("delivery ID changed on retry: %s then %s", prev, d.ID)
			continue
		}
		b.ids[v] = d.ID
		if b.failFirst && !b.failed[v] && strings.HasSuffix(v, "3") {
			b.failed[v] = true
			errs[i] = errors.New("broker unavailable")
			continue
		}
		b.published[stream] = append(b.published[stream], v)
	}
	return errs
}

func (b *broker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, vs := range b.published {
		n += len(vs)
	}
	return n
}

func run(t *testing.T, client *taskmill.Client, ob *outbox.Outbox, pub outbox.Publisher) {
	t.Helper()
	var hs taskmill.Handlers
	ob.AddRelay(&hs, pub)
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers:     &hs,
		Queues:       map[string]taskmill.QueueOptions{ob.Queue(): {Concurrency: 4}},
		PollInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func TestStoreCommitsWithTheTransaction(t *testing.T) {
	t.Parallel()
	client, ob, pool := setup(t)
	b := newBroker(false)
	run(t, client, ob, b)
	ctx := t.Context()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := ob.Store(ctx, taskmill.Pgx(tx), outbox.Message{Topic: "orders", Key: []byte("1"), Value: []byte("rolled back")}); err != nil {
			return err
		}
		return errors.New("roll back")
	})
	if err == nil {
		t.Fatal("expected the rollback error")
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return ob.Store(ctx, taskmill.Pgx(tx), outbox.Message{
			Topic: "orders", Key: []byte("1"), Value: []byte("committed"), Headers: map[string][]byte{"type": []byte("order.paid")},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Had the rolled-back message been stored, it would be published first.
	waitFor(t, func() bool { return b.count() == 1 })
	b.mu.Lock()
	defer b.mu.Unlock()
	if got := b.published["orders/1"]; !slices.Equal(got, []string{"committed"}) {
		t.Fatalf("published %v", b.published)
	}
}

func TestRelayKeepsOrderPerKey(t *testing.T) {
	t.Parallel()
	client, ob, pool := setup(t)
	b := newBroker(true)
	ctx := t.Context()

	const keys, perKey = 3, 15
	for i := range perKey {
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			msgs := make([]outbox.Message, keys)
			for k := range keys {
				msgs[k] = outbox.Message{Topic: "accounts", Key: []byte(strconv.Itoa(k)), Value: fmt.Appendf(nil, "%d-%02d", k, i)}
			}
			return ob.Store(ctx, taskmill.Pgx(tx), msgs...)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	run(t, client, ob, b)

	waitFor(t, func() bool { return b.count() == keys*perKey })
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range keys {
		want := make([]string, perKey)
		for i := range want {
			want[i] = fmt.Sprintf("%d-%02d", k, i)
		}
		if got := b.published[fmt.Sprintf("accounts/%d", k)]; !slices.Equal(got, want) {
			t.Fatalf("key %d: published %v, want %v", k, got, want)
		}
	}
}

func TestStoreRejectsMessagesWithoutTopic(t *testing.T) {
	t.Parallel()
	_, ob, pool := setup(t)
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		return ob.Store(t.Context(), taskmill.Pgx(tx), outbox.Message{Value: []byte("x")})
	})
	if !errors.Is(err, outbox.ErrNoTopic) {
		t.Fatalf("got %v, want ErrNoTopic", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSetupKeepsTheConcurrencyLimit(t *testing.T) {
	t.Parallel()
	client, ob, _ := setup(t)
	ctx := t.Context()
	if err := client.UpdateQueue(ctx, ob.Queue(), &taskmill.QueueSettings{MaxRunning: 3}); err != nil {
		t.Fatal(err)
	}
	if err := ob.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	queues, err := client.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := taskmill.QueueSettings{MaxRunning: 3, DiscardCompleted: true, HaltGroupOnDead: true}
	for _, q := range queues {
		if q.Name == ob.Queue() && q.Settings != want {
			t.Fatalf("got %+v, want %+v", q.Settings, want)
		}
	}
}

func TestAddRelayWithoutPublisherIsReported(t *testing.T) {
	t.Parallel()
	client, ob, _ := setup(t)
	var hs taskmill.Handlers
	ob.AddRelay(&hs, nil)
	_, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{Handlers: &hs})
	if !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}
}

func TestBinaryKeysAndHeadersArriveIntact(t *testing.T) {
	t.Parallel()
	client, ob, pool := setup(t)
	got := make(chan outbox.Delivery, 2)
	run(t, client, ob, outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		for _, d := range ds {
			got <- d
		}
		return make([]error, len(ds))
	}))

	key := []byte{0xff, 0x00, 0x1f, 0x80} // not UTF-8, with a NUL byte
	header := []byte{0x00, 0xfe}
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		return ob.Store(t.Context(), taskmill.Pgx(tx), outbox.Message{
			Topic: "events", Key: key, Value: []byte("v"), Headers: map[string][]byte{"trace": header},
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if !bytes.Equal(d.Key, key) || !bytes.Equal(d.Headers["trace"], header) {
			t.Fatalf("got key %x and header %x", d.Key, d.Headers["trace"])
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the message was not published")
	}
}
