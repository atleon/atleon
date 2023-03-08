package io.atleon.opentracing;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TracingAloTest {

    private final TestTracer tracer = new TestTracer();

    private final TracerFacade tracerFacade = TracerFacade.wrap(tracer);

    @Test
    public void aloWrappedInTracingIsOnlyAcknowledgedOnceWhenMapped() {
        TestAlo alo = new TestAlo("I said what what");
        TracingAlo<String> tracingAlo = TracingAlo.start(alo, tracerFacade, tracerFacade.newSpanBuilder("test"));

        AloFlux.wrap(Flux.just(tracingAlo))
            .map(string -> string.substring(0, 11))
            .map(String::toUpperCase)
            .consumeAloAndGet(Alo::acknowledge)
            .then()
            .block();

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.spans().size());
        assertEquals(1, tracer.spans().get(0).finishCount());
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
            .block();

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.spans().size());
        assertEquals(1, tracer.spans().get(0).finishCount());
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
            .block();

        assertEquals(1, alo.acknowledgeCount());
        assertEquals(0, alo.nacknowledgeCount());
        assertEquals(1, tracer.spans().size());
        assertEquals(1, tracer.spans().get(0).finishCount());
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
            .block();

        assertEquals(1, alo1.acknowledgeCount());
        assertEquals(0, alo1.nacknowledgeCount());
        assertEquals(1, alo2.acknowledgeCount());
        assertEquals(0, alo2.nacknowledgeCount());
        assertEquals(3, tracer.spans().size());
        assertEquals(2, tracer.spans().get(2).references().size());
    }
}