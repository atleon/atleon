package io.atleon.opentelemetry;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Decorates {@link Alo} elements with tracing
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public class TracingAlo<T> extends AbstractDecoratingAlo<T> {

    protected final Tracer tracer;

    protected final Span span;

    private TracingAlo(Alo<T> delegate, Tracer tracer, Span span) {
        super(delegate);
        this.tracer = tracer;
        this.span = span;
    }

    public static <T> TracingAlo<T> start(Alo<T> delegate, Tracer tracer, SpanBuilder spanBuilder) {
        return new TracingAlo<>(delegate, tracer, spanBuilder.startSpan());
    }

    @Override
    public void runInContext(Runnable runnable) {
        try (Scope __ = span.makeCurrent()) {
            delegate.runInContext(runnable);
        }
    }

    @Override
    public <R> R supplyInContext(Supplier<R> supplier) {
        try (Scope __ = span.makeCurrent()) {
            return delegate.supplyInContext(supplier);
        }
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        try (Scope __ = span.makeCurrent()) {
            return new TracingAlo<>(delegate.map(mapper), tracer, span);
        }
    }

    @Override
    public <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos) {
        SpanBuilder spanBuilder = tracer.spanBuilder("atleon.fan.in").setNoParent();
        for (Alo<?> alo : alos) {
            doOnDelegator(alo, TracingAlo.class, it -> spanBuilder.addLink(it.spanContext()));
        }
        return delegate.<R>fanInPropagator(alos).withDecorator(alo -> start(alo, tracer, spanBuilder));
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return delegate.<R>propagator().withDecorator(alo -> new Propagated<>(alo, tracer, span));
    }

    @Override
    public Runnable getAcknowledger() {
        return applySpanTermination(delegate.getAcknowledger(), span);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return applySpanTermination(delegate.getNacknowledger(), span);
    }

    public SpanContext spanContext() {
        return span.getSpanContext();
    }

    private static Runnable applySpanTermination(Runnable acknowledger, Span span) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                span.end();
            }
        };
    }

    private static Consumer<Throwable> applySpanTermination(Consumer<? super Throwable> nacknowledger, Span span) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                span.setStatus(StatusCode.ERROR);
                span.recordException(error);
                span.end();
            }
        };
    }

    /**
     * This extension is necessary to make one-to-many mappings work correctly. The underlying span
     * is propagated to all downstream mappings such that it can be activated in context. However,
     * none of those mappings should finish the span. This is in contrast to "metering" where we
     * do not propagate the meters at all, and simply rely on the originating element's
     * acknowledgement to eventually be executed.
     */
    private static final class Propagated<T> extends TracingAlo<T> {

        public Propagated(Alo<T> delegate, Tracer tracer, Span span) {
            super(delegate, tracer, span);
        }

        @Override
        public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
            try (Scope __ = span.makeCurrent()) {
                return new Propagated<>(delegate.map(mapper), tracer, span);
            }
        }

        @Override
        public Runnable getAcknowledger() {
            return delegate.getAcknowledger();
        }

        @Override
        public Consumer<? super Throwable> getNacknowledger() {
            return delegate.getNacknowledger();
        }
    }
}
