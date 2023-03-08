package io.atleon.opentracing;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility methods for consistently building resources from OpenTracing Tracers
 */
final class TracerFacade {

    private final Tracer tracer;

    private TracerFacade(Tracer tracer) {
        this.tracer = tracer;
    }

    public static TracerFacade global() {
        return wrap(GlobalTracer.get());
    }

    public static TracerFacade wrap(Tracer tracer) {
        return new TracerFacade(Objects.requireNonNull(tracer));
    }

    public Tracer.SpanBuilder newSpanBuilder(String operationName) {
        return tracer.buildSpan(operationName)
            .ignoreActiveSpan()
            .withTag(Tags.COMPONENT, "atleon");
    }

    public <C> Optional<SpanContext> extract(Format<C> format, C carrier) {
        return Optional.ofNullable(tracer.extract(format, carrier));
    }

    public void run(Span span, Runnable runnable) {
        try (Scope ignored = tracer.activateSpan(span)) {
            runnable.run();
        }
    }

    public <T> T supply(Span span, Supplier<T> supplier) {
        try (Scope ignored = tracer.activateSpan(span)) {
            return supplier.get();
        }
    }

    public <T, U, R> R map(Span span, Function<T, U> mapper, Function<Function<T, U>, R> finalizer) {
        try (Scope ignored = tracer.activateSpan(span)) {
            return finalizer.apply(mapper);
        }
    }
}
