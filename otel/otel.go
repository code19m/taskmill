// Package otel instruments taskmill with OpenTelemetry traces and metrics,
// following OpenTelemetry's semantic conventions for messaging.
//
// Create an [Instrumentation] with [New], and add its middleware to the
// client and to the workers:
//
//   - [Instrumentation.EnqueueMiddleware] traces every enqueue with a
//     producer span, "send {queue}", and stores that span's context in the
//     metadata of the tasks.
//   - [Instrumentation.Middleware] traces every attempt with a consumer span,
//     "process {queue}", a child of the producer span: a task's work shows in
//     the trace of the request that enqueued it.
//
// Both record metrics: messaging.client.sent.messages and
// messaging.client.operation.duration for enqueues, and
// messaging.process.duration for attempts. [Instrumentation.ObserveQueues]
// adds gauges of the installation's queues, and [Instrumentation.Publisher]
// traces what the outbox publishes to a broker.
//
// Attributes are those of the conventions, with messaging.system "taskmill"
// and the queue as messaging.destination.name, plus taskmill.task.kind and,
// on spans, taskmill.task.attempt. Metrics carry no task IDs, so their
// cardinality is bounded by the queues and kinds. error.type is the error's
// type as semconv.ErrorType reports it: an error with an ErrorType() string
// method names its own. taskmill's errors name theirs, such as
// "invalid_argument", "panic", or "timeout" for an attempt that ran out of
// time.
package otel

import (
	"context"
	"errors"
	"runtime/debug"
	"slices"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/code19m/taskmill"
)

const (
	scope  = "github.com/code19m/taskmill/otel"
	system = messagingconv.SystemAttr("taskmill")

	kindKey    = attribute.Key("taskmill.task.kind")
	attemptKey = attribute.Key("taskmill.task.attempt")
	stateKey   = attribute.Key("taskmill.task.state")
)

// durationBuckets are the bucket boundaries, in seconds, that the
// conventions advise for messaging durations.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// Config configures [New]. The zero value uses OpenTelemetry's global
// providers and propagator.
type Config struct {
	// TracerProvider creates the tracer. Default: otel.GetTracerProvider().
	TracerProvider trace.TracerProvider

	// MeterProvider creates the meter. Default: otel.GetMeterProvider().
	MeterProvider metric.MeterProvider

	// Propagator carries trace context in task metadata and message headers.
	// Default: otel.GetTextMapPropagator(), which carries nothing until the
	// application sets one, such as propagation.TraceContext{}.
	Propagator propagation.TextMapPropagator
}

// Instrumentation traces and measures taskmill. Create one with [New] and
// share it: it is safe for concurrent use.
type Instrumentation struct {
	tracer     trace.Tracer
	meter      metric.Meter
	propagator propagation.TextMapPropagator

	sent            messagingconv.ClientSentMessages
	enqueueDuration messagingconv.ClientOperationDuration
	processDuration messagingconv.ProcessDuration
}

// New returns an Instrumentation. cfg may be nil. An instrument that cannot
// be created is reported to otel.Handle and records nothing.
func New(cfg *Config) *Instrumentation {
	var c Config
	if cfg != nil {
		c = *cfg
	}
	if c.TracerProvider == nil {
		c.TracerProvider = otel.GetTracerProvider()
	}
	if c.MeterProvider == nil {
		c.MeterProvider = otel.GetMeterProvider()
	}
	if c.Propagator == nil {
		c.Propagator = otel.GetTextMapPropagator()
	}
	version := moduleVersion()
	in := &Instrumentation{
		tracer: c.TracerProvider.Tracer(scope,
			trace.WithInstrumentationVersion(version), trace.WithSchemaURL(semconv.SchemaURL)),
		meter: c.MeterProvider.Meter(scope,
			metric.WithInstrumentationVersion(version), metric.WithSchemaURL(semconv.SchemaURL)),
		propagator: c.Propagator,
	}
	buckets := metric.WithExplicitBucketBoundaries(durationBuckets...)
	var errs [3]error
	in.sent, errs[0] = messagingconv.NewClientSentMessages(in.meter)
	in.enqueueDuration, errs[1] = messagingconv.NewClientOperationDuration(in.meter, buckets)
	in.processDuration, errs[2] = messagingconv.NewProcessDuration(in.meter, buckets)
	if err := errors.Join(errs[:]...); err != nil {
		otel.Handle(err)
	}
	return in
}

// EnqueueMiddleware is a [taskmill.EnqueueMiddleware] that traces each
// enqueue with a producer span, "send {queue}", and stores the span's context
// in the metadata of every task it enqueues, replacing any already there. A
// batch has one span: its messaging.batch.message_count says how many tasks
// it held, and its name and attributes leave out the queue or kind when its
// tasks differ in them. Inside the caller's transaction, the span ends
// before the transaction commits.
func (in *Instrumentation) EnqueueMiddleware(ctx context.Context, tasks []*taskmill.EnqueueTask, next func(context.Context) error) error {
	queue, kind := tasks[0].Queue, tasks[0].Kind
	for _, t := range tasks[1:] {
		if t.Queue != queue {
			queue = ""
		}
		if t.Kind != kind {
			kind = ""
		}
	}
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String(string(system)),
		semconv.MessagingOperationName("send"),
		semconv.MessagingOperationTypeSend,
	}
	var shared []attribute.KeyValue // the attributes that metrics share with the span
	if queue != "" {
		shared = append(shared, semconv.MessagingDestinationName(queue))
	}
	if kind != "" {
		shared = append(shared, kindKey.String(kind))
	}
	attrs = append(attrs, shared...)
	if len(tasks) > 1 {
		attrs = append(attrs, semconv.MessagingBatchMessageCount(len(tasks)))
	}

	start := time.Now()
	ctx, span := in.tracer.Start(ctx, spanName("send", queue),
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
	defer span.End()
	for _, t := range tasks {
		in.propagator.Inject(ctx, propagation.MapCarrier(t.Metadata))
	}

	err := next(ctx)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		errType := semconv.ErrorType(err)
		span.SetAttributes(errType)
		span.SetStatus(codes.Error, err.Error())
		shared = append(shared, errType)
	} else if len(tasks) == 1 {
		span.SetAttributes(semconv.MessagingMessageID(strconv.FormatInt(tasks[0].Result.ID, 10)))
	}

	in.enqueueDuration.Record(ctx, elapsed, "send", system,
		append(slices.Clip(shared), semconv.MessagingOperationTypeSend)...)
	if queue != "" && kind != "" {
		in.sent.Add(ctx, int64(len(tasks)), "send", system, shared...)
		return err
	}
	in.addSent(ctx, tasks, err)
	return err
}

// addSent counts the tasks of a batch that spans several queues or kinds.
func (in *Instrumentation) addSent(ctx context.Context, tasks []*taskmill.EnqueueTask, err error) {
	type stream struct{ queue, kind string }
	counts := make(map[stream]int64)
	for _, t := range tasks {
		counts[stream{t.Queue, t.Kind}]++
	}
	for s, n := range counts {
		attrs := []attribute.KeyValue{semconv.MessagingDestinationName(s.queue), kindKey.String(s.kind)}
		if err != nil {
			attrs = append(attrs, semconv.ErrorType(err))
		}
		in.sent.Add(ctx, n, "send", system, attrs...)
	}
}

// Middleware is a [taskmill.Middleware] that traces each attempt with a
// consumer span, "process {queue}", and measures it. The span's parent is the
// producer span whose context the task's metadata holds, so the attempt shows
// in the trace that enqueued the task, and the span links to it too, as the
// conventions ask; a span already in ctx becomes another link. The handler's
// context carries the span. A handler error sets the span's status and
// error.type, which names why the attempt's context ended if it did, such as
// "timeout"; a snooze is not an error. Add it first, so that it measures the
// other middleware too.
func (in *Instrumentation) Middleware(ctx context.Context, task *taskmill.TaskInfo, next func(context.Context) error) error {
	shared := []attribute.KeyValue{semconv.MessagingDestinationName(task.Queue), kindKey.String(task.Kind)}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(shared...),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String(string(system)),
			semconv.MessagingOperationName("process"),
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingMessageID(strconv.FormatInt(task.ID, 10)),
			attemptKey.Int(task.Attempt),
		),
	}
	ambient := trace.SpanContextFromContext(ctx)
	ctx = in.propagator.Extract(ctx, propagation.MapCarrier(task.Metadata))
	if created := trace.SpanContextFromContext(ctx); created.IsValid() && !created.Equal(ambient) {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: created}))
		if ambient.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ambient}))
		}
	}

	start := time.Now()
	ctx, span := in.tracer.Start(ctx, spanName("process", task.Queue), opts...)
	defer span.End()
	err := next(ctx)
	elapsed := time.Since(start).Seconds()
	if _, snoozed := errors.AsType[*taskmill.SnoozeError](err); err != nil && !snoozed {
		cause := err
		if ctx.Err() != nil {
			cause = context.Cause(ctx) // such as the attempt's timeout
		}
		errType := semconv.ErrorType(cause)
		span.SetAttributes(errType)
		span.SetStatus(codes.Error, err.Error())
		shared = append(shared, errType)
	}
	in.processDuration.Record(ctx, elapsed, "process", system, shared...)
	return err
}

// spanName names a span after its operation and destination, as the
// conventions do: "send emails", or "send" if the destination varies.
func spanName(operation, destination string) string {
	if destination == "" {
		return operation
	}
	return operation + " " + destination
}

// moduleVersion returns this module's version as built into the binary.
func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == scope {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == scope {
			return dep.Version
		}
	}
	return "unknown"
}
