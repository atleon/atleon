package io.atleon.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Facade wrapping a {@link Tracer} created from an {@link OpenTelemetry} instance.
 */
public final class TracerFacade {

    private final Tracer tracer;

    private final TextMapPropagator textMapPropagator;

    private TracerFacade(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("atleon");
        this.textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    public static TracerFacade create(OpenTelemetry openTelemetry) {
        return new TracerFacade(openTelemetry);
    }

    public SpanBuilder newConsumerSpanBuilder(String name, Map<String, String> headers) {
        SpanBuilder spanBuilder = tracer.spanBuilder(name).setNoParent().setSpanKind(SpanKind.CONSUMER);
        return deduceSpanContextToLink(headers).map(spanBuilder::addLink).orElse(spanBuilder);
    }

    public Tracer tracer() {
        return tracer;
    }

    private Optional<SpanContext> deduceSpanContextToLink(Map<String, String> headers) {
        SpanContext currentSpanContext = extractSpanContext(Context.current());
        SpanContext extractedSpanContext = extractSpanContext(extractContext(headers));

        if (!currentSpanContext.isValid()) {
            // No current context, so use the extracted context if available
            return extractedSpanContext.isValid() ? Optional.of(extractedSpanContext) : Optional.empty();
        } else if (!extractedSpanContext.isValid()) {
            // No extracted context, but current context is set, so use it
            return Optional.of(currentSpanContext);
        } else {
            // With both a current and extracted context, use the current context if it represents
            // the same trace as what we have extracted, which likely indicates some tool (probably
            // an agent) has already extracted and set the context under which we are processing.
            return Objects.equals(currentSpanContext.getTraceId(), extractedSpanContext.getTraceId())
                    ? Optional.of(currentSpanContext)
                    : Optional.of(extractedSpanContext);
        }
    }

    private Context extractContext(Map<String, String> headers) {
        return textMapPropagator.extract(Context.root(), headers, StringMapGetter.getInstance());
    }

    private static SpanContext extractSpanContext(Context context) {
        return Span.fromContext(context).getSpanContext();
    }

    private static final class StringMapGetter implements TextMapGetter<Map<String, String>> {

        private static final StringMapGetter INSTANCE = new StringMapGetter();

        private StringMapGetter() {}

        public static StringMapGetter getInstance() {
            return INSTANCE;
        }

        @Override
        public Iterable<String> keys(Map<String, String> map) {
            return map.keySet();
        }

        @Override
        public String get(Map<String, String> map, String key) {
            return map != null ? map.get(key) : null;
        }
    }
}
