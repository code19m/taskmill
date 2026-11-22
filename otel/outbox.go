package otel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
	"go.opentelemetry.io/otel/trace"

	"github.com/code19m/taskmill/outbox"
)

// Publisher wraps pub, the outbox's publisher to a broker, so that each
// delivery is traced with a producer span, "send {topic}", and counted in
// messaging.client.sent.messages. The span's parent is the span that stored
// the message, from [outbox.Delivery.Metadata], and its context is added to
// the message's headers, replacing any already there, so that consumers
// continue the trace. system names the broker as the conventions do for
// messaging.system, such as "kafka" or "rabbitmq". If pub panics, the spans
// of its deliveries end with the panic as their error, of the error.type
// "panic", and the panic continues to the worker, which records it. The
// spans of deliveries that the relay retries for want of a result, as when
// pub returned fewer or more errors than deliveries, end with that error,
// and so do those whose error panics when it is read: the worker then
// fails only those deliveries, as it does without this wrapper. A nil pub
// returns nil: pass it to [outbox.Outbox.AddRelay], and
// [taskmill.NewWorker] reports it.
func (in *Instrumentation) Publisher(system string, pub outbox.Publisher) outbox.Publisher { //nolint:ireturn // it wraps the interface it returns
	if pub == nil {
		return nil
	}

	broker := messagingconv.SystemAttr(system)
	return outbox.PublisherFunc(func(ctx context.Context, deliveries []outbox.Delivery) (errs []error) {
		sent := make([]outbox.Delivery, len(deliveries))
		spans := make([]trace.Span, len(deliveries))
		fields := in.propagator.Fields()
		for i, d := range deliveries {
			parent := in.propagator.Extract(ctx, propagation.MapCarrier(d.Metadata))
			sctx, span := in.tracer.Start(parent, spanName("send", d.Topic),
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(system),
					semconv.MessagingOperationName("send"),
					semconv.MessagingOperationTypeSend,
					semconv.MessagingDestinationName(d.Topic),
					semconv.MessagingMessageID(d.ID),
				))

			headers := make(map[string][]byte, len(d.Headers)+2)
			maps.Copy(headers, d.Headers)
			deleteFields(headers, fields)
			in.propagator.Inject(sctx, headerCarrier(headers))
			d.Headers = headers
			sent[i], spans[i] = d, span
		}

		// The spans end when the publisher returns, and also when it panics:
		// the panic then fails every delivery, and continues. Results that
		// outnumber the deliveries fail every delivery too, as the relay
		// does.
		defer func() {
			panicked := recover()
			for i, span := range spans {
				attrs := []attribute.KeyValue{semconv.MessagingDestinationName(deliveries[i].Topic)}
				err := errMissingResult
				switch {
				case panicked != nil:
					err = panicError{value: panicked}
				case len(errs) > len(deliveries):
					err = fmt.Errorf("the publisher returned %d results for %d deliveries", len(errs), len(deliveries))
				case i < len(errs):
					err = errs[i]
				}

				if err != nil {
					errType, text := describe(err)
					span.SetAttributes(errType)
					span.SetStatus(codes.Error, text)
					attrs = append(attrs, errType)
				}

				span.End()
				in.sent.Add(ctx, 1, "send", broker, attrs...)
			}

			if panicked != nil {
				panic(panicked) // for the worker to record, as it does without this wrapper
			}
		}()

		return pub.Publish(ctx, sent)
	})
}

// errMissingResult fails a delivery for which the publisher returned no
// result, as the relay does.
var errMissingResult = errors.New("the publisher returned no result for this delivery")

// describe returns err's class and text. Both run the methods of the
// publisher's error, which can panic, as the Error method of a nil pointer
// in an error does. Then they name err's type, so that every span still
// ends, and the worker fails only that delivery, as it does without this
// wrapper.
func describe(err error) (class attribute.KeyValue, text string) {
	defer func() {
		if recover() != nil {
			class = semconv.ErrorTypeKey.String(fmt.Sprintf("%T", err))
			text = fmt.Sprintf("a %T that panics when it is formatted", err)
		}
	}()

	return semconv.ErrorType(err), err.Error()
}

// panicError is the error of the deliveries of a publisher that panicked.
// Its class is "panic", as that of taskmill's error for a handler that
// panicked.
type panicError struct {
	value any
}

// Error implements the error interface. Formatting the value runs its own
// methods, which can panic again, as an Error method that panics with its
// receiver does. The text then names the value's type, as the worker's does:
// a second panic here would leave the spans open.
func (e panicError) Error() (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("panic: a %T that panics when it is formatted", e.value)
		}
	}()

	return fmt.Sprintf("panic: %v", e.value)
}

// ErrorType returns the error's class, which semconv.ErrorType reports as
// error.type.
func (panicError) ErrorType() string { return "panic" }

// headerCarrier carries trace context in a message's headers.
type headerCarrier map[string][]byte

func (h headerCarrier) Get(key string) string { return string(h[key]) }

func (h headerCarrier) Set(key, value string) { h[key] = []byte(value) }

func (h headerCarrier) Keys() []string { return slices.Collect(maps.Keys(h)) }
