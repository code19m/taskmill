package otel

import (
	"context"
	"errors"
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
// messaging.system, such as "kafka" or "rabbitmq". A nil pub returns nil,
// which [outbox.Outbox.AddRelay] reports.
func (in *Instrumentation) Publisher(system string, pub outbox.Publisher) outbox.Publisher { //nolint:ireturn // it wraps the interface it returns
	if pub == nil {
		return nil
	}
	broker := messagingconv.SystemAttr(system)
	return outbox.PublisherFunc(func(ctx context.Context, deliveries []outbox.Delivery) []error {
		sent := make([]outbox.Delivery, len(deliveries))
		spans := make([]trace.Span, len(deliveries))
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
			in.propagator.Inject(sctx, headerCarrier(headers))
			d.Headers = headers
			sent[i], spans[i] = d, span
		}

		errs := pub.Publish(ctx, sent)
		for i, span := range spans {
			attrs := []attribute.KeyValue{semconv.MessagingDestinationName(deliveries[i].Topic)}
			err := errMissingResult
			if i < len(errs) {
				err = errs[i]
			}
			if err != nil {
				errType := semconv.ErrorType(err)
				span.SetAttributes(errType)
				span.SetStatus(codes.Error, err.Error())
				attrs = append(attrs, errType)
			}
			span.End()
			in.sent.Add(ctx, 1, "send", broker, attrs...)
		}
		return errs
	})
}

// errMissingResult fails a delivery for which the publisher returned no
// result, as the relay does.
var errMissingResult = errors.New("the publisher returned no result for this delivery")

// headerCarrier carries trace context in a message's headers.
type headerCarrier map[string][]byte

func (h headerCarrier) Get(key string) string { return string(h[key]) }

func (h headerCarrier) Set(key, value string) { h[key] = []byte(value) }

func (h headerCarrier) Keys() []string { return slices.Collect(maps.Keys(h)) }
