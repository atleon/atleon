package io.atleon.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TracerFacadeTest {

    private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

    private final SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();

    private final OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();

    @Test
    public void newConsumerSpanBuilder_givenNoContext_expectsNoLinks() {
        TracerFacade tracerFacade = TracerFacade.create(openTelemetry);

        Map<String, String> headers = Collections.emptyMap();
        tracerFacade.newConsumerSpanBuilder("test", headers).startSpan().end();

        assertEquals(1, spanExporter.getFinishedSpanItems().size());
        assertEquals("test", spanExporter.getFinishedSpanItems().get(0).getName());
        assertTrue(spanExporter.getFinishedSpanItems().get(0).getLinks().isEmpty());
    }

    @Test
    public void newConsumerSpanBuilder_givenCurrentContext_expectsLink() {
        TracerFacade tracerFacade = TracerFacade.create(openTelemetry);

        Span currentSpan = tracerFacade.tracer().spanBuilder("current").startSpan();

        try (Scope __ = currentSpan.makeCurrent()) {
            Map<String, String> headers = Collections.emptyMap();
            tracerFacade.newConsumerSpanBuilder("test", headers).startSpan().end();
        } finally {
            currentSpan.end();
        }

        assertEquals(2, spanExporter.getFinishedSpanItems().size());
        assertEquals("test", spanExporter.getFinishedSpanItems().get(0).getName());
        assertEquals(1, spanExporter.getFinishedSpanItems().get(0).getLinks().size());
        assertEquals(
                currentSpan.getSpanContext().getSpanId(),
                spanExporter
                        .getFinishedSpanItems()
                        .get(0)
                        .getLinks()
                        .get(0)
                        .getSpanContext()
                        .getSpanId());
        assertEquals("current", spanExporter.getFinishedSpanItems().get(1).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(1).getLinks().size());
    }

    @Test
    public void newConsumerSpanBuilder_givenExtractedContext_expectsLink() {
        TracerFacade tracerFacade = TracerFacade.create(openTelemetry);
        TextMapPropagator textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();

        Span producerSpan = tracerFacade.tracer().spanBuilder("producer").startSpan();
        producerSpan.end();

        Map<String, String> headers = new HashMap<>();
        textMapPropagator.inject(producerSpan.storeInContext(Context.root()), headers, (map, key, value) -> {
            if (map != null) {
                map.put(key, value);
            }
        });
        tracerFacade.newConsumerSpanBuilder("test", headers).startSpan().end();

        assertEquals(2, spanExporter.getFinishedSpanItems().size());
        assertEquals("producer", spanExporter.getFinishedSpanItems().get(0).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(0).getLinks().size());
        assertEquals("test", spanExporter.getFinishedSpanItems().get(1).getName());
        assertEquals(1, spanExporter.getFinishedSpanItems().get(1).getLinks().size());
        assertEquals(
                producerSpan.getSpanContext().getSpanId(),
                spanExporter
                        .getFinishedSpanItems()
                        .get(1)
                        .getLinks()
                        .get(0)
                        .getSpanContext()
                        .getSpanId());
    }

    @Test
    public void newConsumerSpanBuilder_givenCurrentAndExtractedContext_expectsLinkToExtractedContext() {
        TracerFacade tracerFacade = TracerFacade.create(openTelemetry);
        TextMapPropagator textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();

        Span producerSpan = tracerFacade.tracer().spanBuilder("producer").startSpan();
        producerSpan.end();

        Map<String, String> headers = new HashMap<>();
        textMapPropagator.inject(producerSpan.storeInContext(Context.root()), headers, (map, key, value) -> {
            if (map != null) {
                map.put(key, value);
            }
        });

        Span currentSpan = tracerFacade.tracer().spanBuilder("current").startSpan();
        try (Scope __ = currentSpan.makeCurrent()) {
            tracerFacade.newConsumerSpanBuilder("test", headers).startSpan().end();
        } finally {
            currentSpan.end();
        }

        assertEquals(3, spanExporter.getFinishedSpanItems().size());
        assertEquals("producer", spanExporter.getFinishedSpanItems().get(0).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(0).getLinks().size());
        assertEquals("test", spanExporter.getFinishedSpanItems().get(1).getName());
        assertEquals(1, spanExporter.getFinishedSpanItems().get(1).getLinks().size());
        assertEquals(
                producerSpan.getSpanContext().getSpanId(),
                spanExporter
                        .getFinishedSpanItems()
                        .get(1)
                        .getLinks()
                        .get(0)
                        .getSpanContext()
                        .getSpanId());
        assertEquals("current", spanExporter.getFinishedSpanItems().get(2).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(2).getLinks().size());
    }

    @Test
    public void newConsumerSpanBuilder_givenMatchingCurrentAndExtractedContext_expectsLinkToCurrentContext() {
        TracerFacade tracerFacade = TracerFacade.create(openTelemetry);
        TextMapPropagator textMapPropagator = openTelemetry.getPropagators().getTextMapPropagator();

        Span producerSpan = tracerFacade.tracer().spanBuilder("producer").startSpan();
        producerSpan.end();

        Map<String, String> headers = new HashMap<>();
        textMapPropagator.inject(producerSpan.storeInContext(Context.root()), headers, (map, key, value) -> {
            if (map != null) {
                map.put(key, value);
            }
        });

        Span currentSpan = tracerFacade
                .tracer()
                .spanBuilder("current")
                .setParent(producerSpan.storeInContext(Context.root()))
                .startSpan();
        try (Scope __ = currentSpan.makeCurrent()) {
            tracerFacade.newConsumerSpanBuilder("test", headers).startSpan().end();
        } finally {
            currentSpan.end();
        }

        assertEquals(3, spanExporter.getFinishedSpanItems().size());
        assertEquals("producer", spanExporter.getFinishedSpanItems().get(0).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(0).getLinks().size());
        assertEquals("test", spanExporter.getFinishedSpanItems().get(1).getName());
        assertEquals(1, spanExporter.getFinishedSpanItems().get(1).getLinks().size());
        assertEquals(
                currentSpan.getSpanContext().getSpanId(),
                spanExporter
                        .getFinishedSpanItems()
                        .get(1)
                        .getLinks()
                        .get(0)
                        .getSpanContext()
                        .getSpanId());
        assertEquals("current", spanExporter.getFinishedSpanItems().get(2).getName());
        assertEquals(0, spanExporter.getFinishedSpanItems().get(2).getLinks().size());
    }
}
