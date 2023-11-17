package io.atleon.opentracing;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.opentracing.mock.MockTracer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TracingAloTest {

    private final MockTracer tracer = new MockTracer();

    private final TracerFacade tracerFacade = TracerFacade.wrap(tracer);

    @Test
    public void aloOperationsEnsureSpansAreActiveInsideFunctions() {
        TestAlo alo1 = new TestAlo("I said what what");
        TestAlo alo2 = new TestAlo("");
        TracingAlo<String> tracingAlo1 = TracingAlo.start(alo1, tracerFacade, tracerFacade.newSpanBuilder("test"));
        TracingAlo<String> tracingAlo2 = TracingAlo.start(alo2, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo1, tracingAlo2))
            .doOnNext(string -> tracerFacade.activeSpan().ifPresent(it -> it.setTag("doneOnNext", true)))
            .filter(string -> {
                tracerFacade.activeSpan().ifPresent(it -> it.setTag("filtered", true));
                return !string.isEmpty();
            })
            .map(string -> {
                tracerFacade.activeSpan().ifPresent(it -> it.setTag("mapped", true));
                return string.toUpperCase();
            })
            .mapNotNull(string -> {
                tracerFacade.activeSpan().ifPresent(it -> it.setTag("mappedNotNull", true));
                return string;
            })
            .consume(string -> {
                tracerFacade.activeSpan().ifPresent(it -> it.setTag("consumed", true));
                throw new UnsupportedOperationException("Cannot do anything else");
            })
            .onAloErrorEmitUnless((object, error) -> {
                tracerFacade.activeSpan().ifPresent(it -> it.setTag("errorIgnored", true));
                return true;
            })
            .doOnDiscard(String.class, string -> tracerFacade.activeSpan().ifPresent(it -> it.setTag("discarded", true)))
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block(Duration.ofSeconds(10));

        assertEquals(2, tracer.finishedSpans().size());

        assertEquals(true, tracer.finishedSpans().get(0).tags().get("doneOnNext"));
        assertEquals(true, tracer.finishedSpans().get(0).tags().get("filtered"));
        assertEquals(true, tracer.finishedSpans().get(0).tags().get("mapped"));
        assertEquals(true, tracer.finishedSpans().get(0).tags().get("mappedNotNull"));
        assertEquals(true, tracer.finishedSpans().get(0).tags().get("consumed"));
        assertEquals(true, tracer.finishedSpans().get(0).tags().get("errorIgnored"));
        assertNull(tracer.finishedSpans().get(0).tags().get("discarded"));

        assertEquals(true, tracer.finishedSpans().get(1).tags().get("doneOnNext"));
        assertEquals(true, tracer.finishedSpans().get(1).tags().get("filtered"));
        assertNull(tracer.finishedSpans().get(1).tags().get("mapped"));
        assertNull(tracer.finishedSpans().get(1).tags().get("mappedNotNull"));
        assertNull(tracer.finishedSpans().get(1).tags().get("consumed"));
        assertNull(tracer.finishedSpans().get(1).tags().get("errorIgnored"));
        assertEquals(true, tracer.finishedSpans().get(1).tags().get("discarded"));
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenMapped() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo))
            .map(string -> string.substring(0, 11))
            .map(String::toUpperCase)
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.finishedSpans().size());
        assertTrue(tracer.finishedSpans().get(0).generatedErrors().isEmpty()); // Error generated if finished more than once
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenPublished() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo))
            .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 11)))
            .concatMap(string -> Mono.just(string).map(String::toUpperCase))
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.finishedSpans().size());
        assertTrue(tracer.finishedSpans().get(0).generatedErrors().isEmpty()); // Error generated if finished more than once
    }

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenMappingAndPublishing() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo))
            .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 11)))
            .map(String::toUpperCase)
            .concatMap(string -> Mono.just(string).map(it -> it.substring(0, 6)))
            .map(String::toLowerCase)
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block(Duration.ofSeconds(10));

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.finishedSpans().size());
        assertTrue(tracer.finishedSpans().get(0).generatedErrors().isEmpty()); // Error generated if finished more than once
    }

    @Test
    public void fannedInAlosResultInMergedSpan() {
        TestAlo alo1 = new TestAlo("I said what what");
        TestAlo alo2 = new TestAlo("in the you know where");

        TracingAlo<String> tracingAlo1 = TracingAlo.start(alo1, tracerFacade, tracerFacade.newSpanBuilder("test"));
        TracingAlo<String> tracingAlo2 = TracingAlo.start(alo2, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo1, tracingAlo2))
            .bufferTimeout(2, Duration.ofNanos(Long.MAX_VALUE))
            .map(strings -> String.join(" ", strings))
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block(Duration.ofSeconds(10));

        assertEquals(1, alo1.acknowledgeCount());
        assertEquals(0, alo1.nacknowledgeCount());
        assertEquals(1, alo2.acknowledgeCount());
        assertEquals(0, alo2.nacknowledgeCount());
        assertEquals(3, tracer.finishedSpans().size());
        assertEquals(2, tracer.finishedSpans().get(2).references().size());
    }
}