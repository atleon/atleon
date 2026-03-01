package io.atleon.opentelemetry;

import io.atleon.core.Alo;
import io.atleon.core.AloDecorator;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanBuilder;

import java.util.Map;

/**
 * Templated implementation of {@link Alo} tracing decoration for consumed data.
 *
 * @param <T> The type of data being consumed
 */
public abstract class TracingAloConsumptionDecorator<T> implements AloDecorator<T> {

    private final TracerFacade tracerFacade;

    protected TracingAloConsumptionDecorator() {
        this(GlobalOpenTelemetry.get());
    }

    protected TracingAloConsumptionDecorator(OpenTelemetry openTelemetry) {
        this.tracerFacade = TracerFacade.create(openTelemetry);
    }

    @Override
    public int order() {
        return INNERMOST_ORDER + 3000;
    }

    @Override
    public final Alo<T> decorate(Alo<T> alo) {
        T t = alo.get();
        Map<String, String> headers = extractHeaderMap(t);
        SpanBuilder spanBuilder = newSpanBuilder(name -> tracerFacade.newConsumerSpanBuilder(name, headers), t);
        return TracingAlo.start(alo, tracerFacade.tracer(), spanBuilder);
    }

    protected abstract SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, T t);

    protected abstract Map<String, String> extractHeaderMap(T t);

    @FunctionalInterface
    protected interface SpanBuilderFactory {
        SpanBuilder newSpanBuilder(String operationName);
    }
}
