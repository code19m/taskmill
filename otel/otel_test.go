package otel_test

import (
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/code19m/taskmill"
	tmotel "github.com/code19m/taskmill/otel"
	"github.com/code19m/taskmill/outbox"
	"github.com/code19m/taskmill/taskmilltest"
)

type sendEmail struct {
	To string `json:"to"`
}

func (sendEmail) Kind() string { return "email.send" }

// env is a client instrumented with in-memory exporters.
type env struct {
	in      *tmotel.Instrumentation
	spans   *tracetest.SpanRecorder
	metrics *sdkmetric.ManualReader
	tracer  trace.Tracer // for the test's own spans, such as a request's
	pool    *pgxpool.Pool
	client  *taskmill.Client
}

func setup(t *testing.T) *env {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	metrics := sdkmetric.NewManualReader()
	in := tmotel.New(&tmotel.Config{
		TracerProvider: tp,
		MeterProvider:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)),
		Propagator:     propagation.TraceContext{},
	})
	pool := testPool(t)
	client := taskmilltest.NewClient(t, pool, &taskmill.ClientConfig{
		EnqueueMiddleware: []taskmill.EnqueueMiddleware{in.EnqueueMiddleware},
	})
	return &env{in: in, spans: spans, metrics: metrics, tracer: tp.Tracer("test"), pool: pool, client: client}
}

// testPool connects to the database named by TASKMILL_TEST_DATABASE_URL, as
// the core's tests do, or skips the test if it is unset.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TASKMILL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TASKMILL_TEST_DATABASE_URL is not set; run `make db` and `make test`")
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// worker returns a worker with the instrumentation's middleware that runs
// tasks with RunTask.
func (e *env) worker(t *testing.T, hs *taskmill.Handlers) *taskmill.Worker {
	t.Helper()
	w, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{
		Handlers:     hs,
		Middleware:   []taskmill.Middleware{e.in.Middleware},
		NoMaintainer: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// ended returns the ended spans with the given name, in the order they ended.
func (e *env) ended(name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range e.spans.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// one returns the only ended span with the given name.
func (e *env) one(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	spans := e.ended(name)
	if len(spans) != 1 {
		t.Fatalf("got %d spans named %q, want 1", len(spans), name)
	}
	return spans[0]
}

// wantAttrs fails the test unless span has every attribute in want, written
// as "key=value".
func wantAttrs(t *testing.T, span sdktrace.ReadOnlySpan, want ...string) {
	t.Helper()
	got := render(span.Attributes())
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("span %q: no attribute %s in %v", span.Name(), w, got)
		}
	}
}

func render(attrs []attribute.KeyValue) []string {
	out := make([]string, len(attrs))
	for i, kv := range attrs {
		out[i] = string(kv.Key) + "=" + kv.Value.String()
	}
	slices.Sort(out)
	return out
}

// points collects the metric with the given name, and renders each of its
// points as its sorted attributes followed by its value: the sum of a
// counter, the value of a gauge, or the count of a histogram.
func (e *env) points(t *testing.T, name string) []string {
	t.Helper()
	var out []string
	switch data := e.collect(t, name).(type) {
	case nil:
	case metricdata.Sum[int64]:
		out = values(data.DataPoints)
	case metricdata.Gauge[int64]:
		out = values(data.DataPoints)
	case metricdata.Gauge[float64]:
		out = values(data.DataPoints)
	case metricdata.Histogram[float64]:
		for _, p := range data.DataPoints {
			out = append(out, point(p.Attributes, p.Count))
		}
	default:
		t.Fatalf("metric %s has data of type %T", name, data)
	}
	slices.Sort(out)
	return out
}

// collect returns the data of the metric with the given name, or nil.
func (e *env) collect(t *testing.T, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.metrics.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

func values[N int64 | float64](points []metricdata.DataPoint[N]) []string {
	out := make([]string, len(points))
	for i, p := range points {
		out[i] = point(p.Attributes, p.Value)
	}
	return out
}

func point(set attribute.Set, value any) string {
	return fmt.Sprintf("%s: %v", strings.Join(render(set.ToSlice()), " "), value)
}

func wantPoints(t *testing.T, name string, got, want []string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s:\ngot  %q\nwant %q", name, got, want)
	}
}

func TestTraceFollowsTheTaskFromEnqueueToAttempts(t *testing.T) {
	t.Parallel()
	e := setup(t)

	ctx, request := e.tracer.Start(t.Context(), "request")
	var id int64
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		res, err := e.client.EnqueueTx(ctx, taskmill.Pgx(tx), sendEmail{}, &taskmill.EnqueueOptions{Queue: "emails"})
		id = res.ID
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	request.End()

	var handlerSpans []trace.SpanContext
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		handlerSpans = append(handlerSpans, trace.SpanContextFromContext(ctx))
		if task.Attempt == 1 {
			return errors.New("smtp unavailable")
		}
		return nil
	}), nil)
	w := e.worker(t, &hs)
	for range 2 { // a failed attempt, then a retry
		if _, err := w.RunTask(t.Context(), id); err != nil {
			t.Fatal(err)
		}
	}

	send := e.one(t, "send emails")
	if send.SpanKind() != trace.SpanKindProducer || send.Parent().SpanID() != request.SpanContext().SpanID() {
		t.Errorf("send span: kind %s, parent %s; want a producer child of the request", send.SpanKind(), send.Parent().SpanID())
	}
	taskID := strconv.FormatInt(id, 10)
	wantAttrs(t, send, "messaging.system=taskmill", "messaging.operation.name=send", "messaging.operation.type=send",
		"messaging.destination.name=emails", "taskmill.task.kind=email.send", "messaging.message.id="+taskID)

	attempts := e.ended("process emails")
	if len(attempts) != 2 || len(handlerSpans) != 2 {
		t.Fatalf("got %d process spans and %d handler calls, want 2 of each", len(attempts), len(handlerSpans))
	}
	for i, span := range attempts {
		if !handlerSpans[i].Equal(span.SpanContext()) {
			t.Errorf("attempt %d: the handler's context does not carry its span", i+1)
		}
		wantProcess(t, span, send)
		wantAttrs(t, span, "messaging.message.id="+taskID, "taskmill.task.attempt="+strconv.Itoa(i+1))
	}
	if s := attempts[0].Status(); s.Code != codes.Error || s.Description != "smtp unavailable" {
		t.Errorf("failed attempt: got status %+v", s)
	}
	wantAttrs(t, attempts[0], "error.type=*errors.errorString")
	if s := attempts[1].Status(); s.Code != codes.Unset {
		t.Errorf("successful attempt: got status %+v", s)
	}
}

func TestMetricsCountEnqueuesAndAttempts(t *testing.T) {
	t.Parallel()
	e := setup(t)
	ctx := t.Context()

	single, err := e.client.Enqueue(ctx, sendEmail{To: "panic"}, &taskmill.EnqueueOptions{Queue: "emails"})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := e.client.EnqueueMany(ctx, []taskmill.EnqueueParams{
		{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Queue: "emails"}},
		{Args: sendEmail{}, Options: &taskmill.EnqueueOptions{Queue: "reports"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := &taskmill.EnqueueOptions{Queue: "emails", Metadata: map[string]string{"bad": "\x00"}}
	if _, err := e.client.Enqueue(ctx, sendEmail{}, invalid); !errors.Is(err, taskmill.ErrInvalidArgument) {
		t.Fatalf("got %v, want ErrInvalidArgument", err)
	}

	// A batch across queues has one span, whose context every task carries.
	span := e.one(t, "send")
	wantAttrs(t, span, "messaging.batch.message_count=2", "taskmill.task.kind=email.send")
	for _, res := range batch {
		info, err := e.client.Get(ctx, res.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(info.Metadata["traceparent"], span.SpanContext().SpanID().String()) {
			t.Errorf("task %d: traceparent %q does not name the batch's span", res.ID, info.Metadata["traceparent"])
		}
	}
	if failed := e.ended("send emails"); len(failed) != 2 || failed[1].Status().Code != codes.Error {
		t.Errorf("the failed enqueue's span has no error status")
	}

	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(ctx context.Context, task *taskmill.Task[sendEmail]) error {
		switch task.Args.To {
		case "panic":
			panic("boom")
		case "slow":
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}), &taskmill.HandlerOptions{Timeout: 100 * time.Millisecond})
	w := e.worker(t, &hs)
	slow, err := e.client.Enqueue(ctx, sendEmail{To: "slow"}, &taskmill.EnqueueOptions{Queue: "reports"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{single.ID, batch[1].ID, slow.ID} {
		if _, err := w.RunTask(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	invalidType := "error.type=invalid_argument "
	wantPoints(t, "sent", e.points(t, "messaging.client.sent.messages"), []string{
		"messaging.destination.name=emails messaging.operation.name=send messaging.system=taskmill taskmill.task.kind=email.send: 2",
		"messaging.destination.name=reports messaging.operation.name=send messaging.system=taskmill taskmill.task.kind=email.send: 2",
		invalidType + "messaging.destination.name=emails messaging.operation.name=send messaging.system=taskmill taskmill.task.kind=email.send: 1",
	})
	wantPoints(t, "enqueue duration", e.points(t, "messaging.client.operation.duration"), []string{
		"messaging.destination.name=emails messaging.operation.name=send messaging.operation.type=send messaging.system=taskmill taskmill.task.kind=email.send: 1",
		"messaging.destination.name=reports messaging.operation.name=send messaging.operation.type=send messaging.system=taskmill taskmill.task.kind=email.send: 1",
		"messaging.operation.name=send messaging.operation.type=send messaging.system=taskmill taskmill.task.kind=email.send: 1",
		invalidType + "messaging.destination.name=emails messaging.operation.name=send messaging.operation.type=send messaging.system=taskmill taskmill.task.kind=email.send: 1",
	})
	wantPoints(t, "process duration", e.points(t, "messaging.process.duration"), []string{
		"error.type=panic messaging.destination.name=emails messaging.operation.name=process messaging.system=taskmill taskmill.task.kind=email.send: 1",
		"error.type=timeout messaging.destination.name=reports messaging.operation.name=process messaging.system=taskmill taskmill.task.kind=email.send: 1",
		"messaging.destination.name=reports messaging.operation.name=process messaging.system=taskmill taskmill.task.kind=email.send: 1",
	})
}

func TestObserveQueuesReportsCountsAndLag(t *testing.T) {
	t.Parallel()
	e := setup(t)
	reg, err := e.in.ObserveQueues(e.client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Unregister() })
	for range 3 {
		_, err := e.client.Enqueue(t.Context(), sendEmail{}, &taskmill.EnqueueOptions{
			Queue: "backlog", RunAt: time.Now().Add(-time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// A worker that serves another queue leads, and counts the backlog.
	var hs taskmill.Handlers
	taskmill.AddHandler(&hs, taskmill.HandlerFunc[sendEmail](func(context.Context, *taskmill.Task[sendEmail]) error { return nil }), nil)
	w, err := taskmill.NewWorker(e.client, &taskmill.WorkerConfig{Handlers: &hs, Queues: map[string]taskmill.QueueOptions{"other": {}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stop := sync.OnceFunc(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(stop)

	const available = "messaging.destination.name=backlog taskmill.task.state=available: 3"
	var counts []string
	eventually(t, "the backlog to be counted", func() bool {
		counts = e.points(t, "taskmill.queue.tasks")
		return slices.Contains(counts, available)
	})
	for _, s := range []string{"scheduled", "pending", "running", "retryable", "dead", "canceled"} {
		if want := "messaging.destination.name=backlog taskmill.task.state=" + s + ": 0"; !slices.Contains(counts, want) {
			t.Errorf("no zero for state %s in %q", s, counts)
		}
	}

	if capped := e.points(t, "taskmill.queue.capped"); !slices.Contains(capped, "messaging.destination.name=backlog: 0") {
		t.Errorf("got capped %q, want 0 for the backlog", capped)
	}
	if lag := e.value(t, "taskmill.queue.lag", "messaging.destination.name=backlog"); lag < 3600 || lag > 3660 {
		t.Errorf("got lag %v seconds, want about an hour", lag)
	}
	age := e.value(t, "taskmill.stats.age", "")
	if age < 0 || age > 15 {
		t.Errorf("got statistics %v seconds old, want a fresh count", age)
	}

	// Without a maintainer, the statistics age.
	stop()
	eventually(t, "the statistics to age", func() bool {
		return e.value(t, "taskmill.stats.age", "") > age+0.5
	})
}

// value returns the value of the point of a gauge whose attributes render as
// attrs, or fails the test if there is none.
func (e *env) value(t *testing.T, name, attrs string) float64 {
	t.Helper()
	for _, p := range e.points(t, name) {
		if rest, ok := strings.CutPrefix(p, attrs+": "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("%s has no point with attributes %q", name, attrs)
	return 0
}

// eventually polls cond until it holds, for up to 30 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestPublisherContinuesTheTraceToTheBroker(t *testing.T) {
	t.Parallel()
	e := setup(t)
	ob := outbox.New(e.client)

	var got []outbox.Delivery
	broker := outbox.PublisherFunc(func(_ context.Context, ds []outbox.Delivery) []error {
		errs := make([]error, len(ds))
		for i, d := range ds {
			got = append(got, d)
			if string(d.Value) == "rejected" {
				errs[i] = errors.New("broker rejected the message")
			}
		}
		return errs
	})
	var hs taskmill.Handlers
	ob.AddRelay(&hs, e.in.Publisher("kafka", broker), nil)
	w := e.worker(t, &hs)

	ctx, request := e.tracer.Start(t.Context(), "request")
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		return ob.Store(ctx, taskmill.Pgx(tx),
			outbox.Message{Topic: "orders", Value: []byte("accepted"), Headers: map[string][]byte{"origin": []byte("shop")}},
			outbox.Message{Topic: "orders", Value: []byte("rejected")})
	})
	if err != nil {
		t.Fatal(err)
	}
	request.End()
	// The schema holds only the outbox's tasks. List takes no filter here:
	// its type differs between the core next to this module and the release
	// that the module requires.
	page, err := e.client.List(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(page.Tasks) // List returns the newest first
	for _, task := range page.Tasks {
		if _, err := w.RunTask(t.Context(), task.ID); err != nil {
			t.Fatal(err)
		}
	}

	stored := e.one(t, "send outbox")
	sends := e.ended("send orders")
	if len(got) != 2 || len(sends) != 2 {
		t.Fatalf("got %d deliveries and %d send spans, want 2 of each", len(got), len(sends))
	}
	for i, span := range sends {
		wantSend(t, span, got[i], stored)
	}
	if string(got[0].Headers["origin"]) != "shop" {
		t.Errorf("the message's own header was lost: %q", got[0].Headers)
	}
	if sends[0].Status().Code != codes.Unset || sends[1].Status().Code != codes.Error {
		t.Errorf("got statuses %v and %v, want unset and error", sends[0].Status(), sends[1].Status())
	}
	for _, span := range e.ended("process outbox") {
		if span.Parent().SpanID() != stored.SpanContext().SpanID() {
			t.Errorf("the relay's attempt is not a child of the span that stored the message")
		}
	}
	wantPoints(t, "sent to the broker", slices.DeleteFunc(e.points(t, "messaging.client.sent.messages"),
		func(p string) bool { return strings.Contains(p, "messaging.system=taskmill") }), []string{
		"error.type=*errors.errorString messaging.destination.name=orders messaging.operation.name=send messaging.system=kafka: 1",
		"messaging.destination.name=orders messaging.operation.name=send messaging.system=kafka: 1",
	})
}

// wantProcess fails the test unless span traces an attempt of a task of the
// emails queue as a consumer child of send, linked to it.
func wantProcess(t *testing.T, span, send sdktrace.ReadOnlySpan) {
	t.Helper()
	if span.SpanKind() != trace.SpanKindConsumer || span.Parent().SpanID() != send.SpanContext().SpanID() {
		t.Errorf("process span: kind %s, parent %s; want a consumer child of the send span", span.SpanKind(), span.Parent().SpanID())
	}
	if links := span.Links(); len(links) != 1 || links[0].SpanContext.SpanID() != send.SpanContext().SpanID() {
		t.Errorf("process span: got links %v, want one to the send span", links)
	}
	wantAttrs(t, span, "messaging.system=taskmill", "messaging.operation.name=process", "messaging.operation.type=process",
		"messaging.destination.name=emails", "taskmill.task.kind=email.send")
}

// wantSend fails the test unless span traces the publishing of d, as a child
// of the span that stored it, and d's headers carry span's context.
func wantSend(t *testing.T, span sdktrace.ReadOnlySpan, d outbox.Delivery, stored sdktrace.ReadOnlySpan) {
	t.Helper()
	if span.SpanKind() != trace.SpanKindProducer || span.Parent().SpanID() != stored.SpanContext().SpanID() {
		t.Errorf("send of %s: kind %s, parent %s; want a producer child of the span that stored it",
			d.Value, span.SpanKind(), span.Parent().SpanID())
	}
	wantAttrs(t, span, "messaging.system=kafka", "messaging.operation.name=send",
		"messaging.destination.name=orders", "messaging.message.id="+d.ID)
	if !strings.Contains(string(d.Headers["traceparent"]), span.SpanContext().SpanID().String()) {
		t.Errorf("delivery of %s: traceparent %q does not name its send span", d.Value, d.Headers["traceparent"])
	}
}
