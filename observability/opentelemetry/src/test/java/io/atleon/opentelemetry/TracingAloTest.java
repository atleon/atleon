package io.atleon.opentelemetry;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TracingAloTest {

    private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

    private final Tracer tracer = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
            .get("test");

    @Test
    public void aloOperationsEnsureSpansAreActiveInsideFunctions() {
        TestAlo alo1 = new TestAlo("I said what what");
        TestAlo alo2 = new TestAlo("");
        TracingAlo<String> tracingAlo1 = TracingAlo.start(alo1, tracer, tracer.spanBuilder("test"));
        TracingAlo<String> tracingAlo2 = TracingAlo.start(alo2, tracer, tracer.spanBuilder("test"));

        AloFlux.just(tracingAlo1, tracingAlo2)
                .doOnNext(string -> Span.current().setAttribute("doneOnNext", true))
                .filter(string -> {
                    Span.current().setAttribute("filtered", true);
                    return !string.isEmpty();
                })
                .map(string -> {
                    Span.current().setAttribute("mapped", true);
                    return string.toUpperCase();
                })
                .mapNotNull(string -> {
                    Span.current().setAttribute("mappedNotNull", true);
                    return string;
                })
                .consume(string -> {
                    Span.current().setAttribute("consumed", true);
                    throw new UnsupportedOperationException("Cannot do anything else");
                })
                .onAloErrorEmitUnless((object, error) -> {
                    Span.current().setAttribute("errorIgnored", true);
                    return true;
                })
                .doOnDiscard(String.class, string -> Span.current().setAttribute("discarded", true))
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(2, spanExporter.getFinishedSpanItems().size());

        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("doneOnNext")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("filtered")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("mapped")));
        assertEquals(
                true,
                spanExporter
                        .getFinishedSpanItems()
                        .get(0)
                        .getAttributes()
                        .get(AttributeKey.booleanKey("mappedNotNull")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("consumed")));
        assertEquals(
                true,
                spanExporter
                        .getFinishedSpanItems()
                        .get(0)
                        .getAttributes()
                        .get(AttributeKey.booleanKey("errorIgnored")));
        assertNull(
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("discarded")));

        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("doneOnNext")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("filtered")));
        assertNull(spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("mapped")));
        assertNull(spanExporter
                .getFinishedSpanItems()
                .get(1)
                .getAttributes()
                .get(AttributeKey.booleanKey("mappedNotNull")));
        assertNull(spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("consumed")));
        assertNull(spanExporter
                .getFinishedSpanItems()
                .get(1)
                .getAttributes()
                .get(AttributeKey.booleanKey("errorIgnored")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("discarded")));
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenMapped() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracer, tracer.spanBuilder("test"));

        AloFlux.just(tracingAlo)
                .map(string -> string.substring(0, 11))
                .map(String::toUpperCase)
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
        assertNotEquals(
                StatusCode.ERROR,
                spanExporter.getFinishedSpanItems().get(0).getStatus().getStatusCode());
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenPublished() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracer, tracer.spanBuilder("test"));

        AloFlux.just(tracingAlo)
                .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 11)))
                .concatMap(string -> Mono.just(string).map(String::toUpperCase))
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
        assertNotEquals(
                StatusCode.ERROR,
                spanExporter.getFinishedSpanItems().get(0).getStatus().getStatusCode());
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenMappingAndPublishing() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracer, tracer.spanBuilder("test"));

        AloFlux.just(tracingAlo)
                .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 11)))
                .map(String::toUpperCase)
                .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 6)))
                .map(String::toLowerCase)
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
        assertNotEquals(
                StatusCode.ERROR,
                spanExporter.getFinishedSpanItems().get(0).getStatus().getStatusCode());
    }

    @Test
    public void fannedInAlosResultInMergedSpan() {
        TestAlo alo1 = new TestAlo("I said what what");
        TestAlo alo2 = new TestAlo("in the you know where");

        TracingAlo<String> tracingAlo1 = TracingAlo.start(alo1, tracer, tracer.spanBuilder("test"));
        TracingAlo<String> tracingAlo2 = TracingAlo.start(alo2, tracer, tracer.spanBuilder("test"));

        AloFlux.just(tracingAlo1, tracingAlo2)
                .bufferTimeout(2, Duration.ofNanos(Long.MAX_VALUE))
                .map(strings -> String.join(" ", strings))
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(1, alo1.acknowledgeCount());
        assertEquals(0, alo1.nacknowledgeCount());
        assertEquals(1, alo2.acknowledgeCount());
        assertEquals(0, alo2.nacknowledgeCount());
        assertEquals(3, spanExporter.getFinishedSpanItems().size());
        assertEquals(2, spanExporter.getFinishedSpanItems().get(2).getLinks().size());
    }

    @Test
    public void wrappedDelegatesAreAppropriatelyDelegatedTo() {
        TestAlo alo = new TestAlo("How much wood would a woodchuck chuck");

        TracingAlo<String> inner = TracingAlo.start(alo, tracer, tracer.spanBuilder("inner"));
        TracingAlo<String> outer = TracingAlo.start(inner, tracer, tracer.spanBuilder("outer"));

        AloFlux.just(outer)
                .filter(__ -> {
                    Span.current().setAttribute("filtered", true);
                    return true;
                })
                .doOnNext(__ -> Span.current().setAttribute("doneOnNext", true))
                .consumeAloAndGet(Alo::acknowledge)
                .then()
                .block(Duration.ofSeconds(10));

        assertEquals(2, spanExporter.getFinishedSpanItems().size());
        assertEquals("inner", spanExporter.getFinishedSpanItems().get(0).getName());
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("filtered")));
        assertEquals(
                true,
                spanExporter.getFinishedSpanItems().get(0).getAttributes().get(AttributeKey.booleanKey("doneOnNext")));
        assertEquals("outer", spanExporter.getFinishedSpanItems().get(1).getName());
        assertNull(spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("filtered")));
        assertNull(
                spanExporter.getFinishedSpanItems().get(1).getAttributes().get(AttributeKey.booleanKey("doneOnNext")));
    }
}
