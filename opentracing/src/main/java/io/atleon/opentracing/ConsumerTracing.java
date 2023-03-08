package io.atleon.opentracing;

import io.atleon.util.Configurable;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;

import java.util.Map;
import java.util.Optional;

public abstract class ConsumerTracing implements Configurable {

    private final TracerFacade tracerFacade = TracerFacade.global();

    protected Tracer.SpanBuilder newSpanBuilder(String operationName) {
        return tracerFacade.newSpanBuilder(operationName)
            .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER);
    }

    protected Optional<SpanContext> extractSpanContext(Map<String, String> headerMap) {
        return tracerFacade.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(headerMap));
    }

    protected TracerFacade tracerFacade() {
        return tracerFacade;
    }
}
