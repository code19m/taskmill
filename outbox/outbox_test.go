package outbox_test

import (
	"context"
	"errors"
	"fmt"
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

func setup(t *testing.T) (*taskmill.Client, *pgxpool.Pool) {
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
	if err := outbox.Setup(t.Context(), client, nil); err != nil {
		t.Fatal(err)
	}
	return client, pool
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
		stream := d.Topic + "/" + d.Key
		if d.Key != "" && seen[stream] {
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

func run(t *testing.T, client *taskmill.Client, pub outbox.Publisher) {
	t.Helper()
	var hs taskmill.Handlers
	outbox.AddRelay(&hs, pub, &outbox.Options{BatchSize: 16})
	w, err := taskmill.NewWorker(client, &taskmill.WorkerConfig{
		Handlers:     &hs,
		Queues:       map[string]taskmill.QueueOptions{outbox.DefaultQueue: {Concurrency: 4}},
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
	client, pool := setup(t)
	b := newBroker(false)
	run(t, client, b)
	ctx := t.Context()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := outbox.Store(ctx, client, taskmill.Pgx(tx), nil, outbox.Message{Topic: "orders", Key: "1", Value: []byte("rolled back")}); err != nil {
			return err
		}
		return errors.New("roll back")
	})
	if err == nil {
		t.Fatal("expected the rollback error")
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return outbox.Store(ctx, client, taskmill.Pgx(tx), nil, outbox.Message{
			Topic: "orders", Key: "1", Value: []byte("committed"), Headers: map[string]string{"type": "order.paid"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return b.count() == 1 })
	time.Sleep(200 * time.Millisecond)
	b.mu.Lock()
	defer b.mu.Unlock()
	if got := b.published["orders/1"]; !slices.Equal(got, []string{"committed"}) {
		t.Fatalf("published %v", b.published)
	}
}

func TestRelayKeepsOrderPerKey(t *testing.T) {
	t.Parallel()
	client, pool := setup(t)
	b := newBroker(true)
	ctx := t.Context()

	const keys, perKey = 3, 15
	for i := range perKey {
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			msgs := make([]outbox.Message, keys)
			for k := range keys {
				msgs[k] = outbox.Message{Topic: "accounts", Key: strconv.Itoa(k), Value: fmt.Appendf(nil, "%d-%02d", k, i)}
			}
			return outbox.Store(ctx, client, taskmill.Pgx(tx), nil, msgs...)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	run(t, client, b)

	waitFor(t, func() bool { return b.count() == keys*perKey })
	b.mu.Lock()
	defer b.mu.Unlock()
	for k := range keys {
		got := b.published[fmt.Sprintf("accounts/%d", k)]
		if !slices.IsSorted(got) || len(got) != perKey {
			t.Fatalf("key %d published out of order: %v", k, got)
		}
	}
}

func TestStoreRejectsMessagesWithoutTopic(t *testing.T) {
	t.Parallel()
	client, pool := setup(t)
	err := pgx.BeginFunc(t.Context(), pool, func(tx pgx.Tx) error {
		return outbox.Store(t.Context(), client, taskmill.Pgx(tx), nil, outbox.Message{Value: []byte("x")})
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
