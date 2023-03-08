package io.atleon.opentracing;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTracer implements Tracer {

    private final List<MockSpan> spans = new ArrayList<>();

    @Override
    public ScopeManager scopeManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Span activeSpan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scope activateSpan(Span span) {
        return new MockScope(MockSpan.class.cast(span));
    }

    @Override
    public SpanBuilder buildSpan(String operationName) {
        return new MockSpanBuilder(operationName);
    }

    @Override
    public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C> SpanContext extract(Format<C> format, C carrier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    public List<MockSpan> spans() {
        return spans;
    }

    public final class MockSpanBuilder implements SpanBuilder {

        private final String operationName;

        private final List<ReferencedSpanContext> references = new ArrayList<>();

        public MockSpanBuilder(String operationName) {
            this.operationName = operationName;
        }

        @Override
        public SpanBuilder asChildOf(SpanContext parent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SpanBuilder asChildOf(Span parent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
            references.add(new ReferencedSpanContext(referenceType, referencedContext));
            return this;
        }

        @Override
        public SpanBuilder ignoreActiveSpan() {
            return this;
        }

        @Override
        public SpanBuilder withTag(String key, String value) {
            return this;
        }

        @Override
        public SpanBuilder withTag(String key, boolean value) {
            return this;
        }

        @Override
        public SpanBuilder withTag(String key, Number value) {
            return this;
        }

        @Override
        public <T> SpanBuilder withTag(Tag<T> tag, T value) {
            return this;
        }

        @Override
        public SpanBuilder withStartTimestamp(long microseconds) {
            return this;
        }

        @Override
        public Span startManual() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Span start() {
            MockSpan span = new MockSpan(references);
            spans.add(span);
            return span;
        }

        @Override
        public Scope startActive(boolean finishSpanOnClose) {
            throw new UnsupportedOperationException();
        }

        public String operationName() {
            return operationName;
        }
    }

    public static final class MockSpan implements Span {

        private final UUID traceId = UUID.randomUUID();

        private final UUID spanId = UUID.randomUUID();

        private final List<ReferencedSpanContext> references;

        private final AtomicInteger finishCount = new AtomicInteger(0);

        public MockSpan(List<ReferencedSpanContext> references) {
            this.references = references;
        }

        @Override
        public SpanContext context() {
            return new MockSpanContext(traceId, spanId);
        }

        @Override
        public Span setTag(String key, String value) {
            return this;
        }

        @Override
        public Span setTag(String key, boolean value) {
            return this;
        }

        @Override
        public Span setTag(String key, Number value) {
            return this;
        }

        @Override
        public <T> Span setTag(Tag<T> tag, T value) {
            return this;
        }

        @Override
        public Span log(Map<String, ?> fields) {
            return this;
        }

        @Override
        public Span log(long timestampMicroseconds, Map<String, ?> fields) {
            return this;
        }

        @Override
        public Span log(String event) {
            return this;
        }

        @Override
        public Span log(long timestampMicroseconds, String event) {
            return this;
        }

        @Override
        public Span setBaggageItem(String key, String value) {
            return this;
        }

        @Override
        public String getBaggageItem(String key) {
            return null;
        }

        @Override
        public Span setOperationName(String operationName) {
            return this;
        }

        @Override
        public void finish() {
            finishCount.incrementAndGet();
        }

        @Override
        public void finish(long finishMicros) {
            finish();
        }

        public List<ReferencedSpanContext> references() {
            return references;
        }

        public int finishCount() {
            return finishCount.get();
        }
    }

    public static final class MockSpanContext implements SpanContext {

        private final UUID traceId;

        private final UUID spanId;

        public MockSpanContext(UUID traceId, UUID spanId) {
            this.traceId = traceId;
            this.spanId = spanId;
        }

        @Override
        public String toTraceId() {
            return traceId.toString();
        }

        @Override
        public String toSpanId() {
            return spanId.toString();
        }

        @Override
        public Iterable<Map.Entry<String, String>> baggageItems() {
            return Collections.emptyList();
        }
    }

    public static final class MockScope implements Scope {

        private final MockSpan span;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        public MockScope(MockSpan span) {
            this.span = span;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public MockSpan span() {
            return span;
        }
    }

    private static final class ReferencedSpanContext {

        private final String referenceType;

        private final SpanContext spanContext;

        private ReferencedSpanContext(String referenceType, SpanContext spanContext) {
            this.referenceType = referenceType;
            this.spanContext = spanContext;
        }

        public String referenceType() {
            return referenceType;
        }

        public SpanContext spanContext() {
            return spanContext;
        }
    }
}
