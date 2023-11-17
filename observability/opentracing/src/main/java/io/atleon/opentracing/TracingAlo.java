package io.atleon.opentracing;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.DelegatingAlo;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Decorates {@link Alo} elements with tracing
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public class TracingAlo<T> extends AbstractDecoratingAlo<T> {

    protected final TracerFacade tracerFacade;

    protected final Span span;

    private TracingAlo(Alo<T> delegate, TracerFacade tracerFacade, Span span) {
        super(delegate);
        this.tracerFacade = tracerFacade;
        this.span = span;
    }

    public static <T> TracingAlo<T> start(Alo<T> delegate, TracerFacade tracerFacade, Tracer.SpanBuilder spanBuilder) {
        return new TracingAlo<>(delegate, tracerFacade, spanBuilder.start());
    }

    @Override
    public void runInContext(Runnable runnable) {
        tracerFacade.run(span, runnable);
    }

    @Override
    public <R> R supplyInContext(Supplier<R> supplier) {
        return tracerFacade.supply(span, supplier);
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        Alo<R> result = tracerFacade.map(span, mapper, delegate::map);
        return new TracingAlo<>(result, tracerFacade, span);
    }

    @Override
    public <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos) {
        Tracer.SpanBuilder spanBuilder = tracerFacade.newSpanBuilder("atleon.fan.in");
        for (Alo<?> alo : alos) {
            extractSpanContext(alo).ifPresent(it -> spanBuilder.addReference(References.CHILD_OF, it));
        }
        return delegate.<R>fanInPropagator(alos).withDecorator(alo -> start(alo, tracerFacade, spanBuilder));
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return delegate.<R>propagator().withDecorator(alo -> new Propagated<>(alo, tracerFacade, span));
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
        return span.context();
    }

    private static Optional<SpanContext> extractSpanContext(Alo<?> alo) {
        if (alo instanceof TracingAlo) {
            return Optional.of(TracingAlo.class.cast(alo).spanContext());
        } else if (alo instanceof DelegatingAlo) {
            return extractSpanContext(DelegatingAlo.class.cast(alo).getDelegate());
        } else {
            return Optional.empty();
        }
    }

    private static Runnable applySpanTermination(Runnable acknowledger, Span span) {
        return () -> {
            try {
                acknowledger.run();
            } finally {
                span.setTag(Tags.ERROR, false);
                span.finish();
            }
        };
    }

    private static Consumer<Throwable> applySpanTermination(Consumer<? super Throwable> nacknowledger, Span span) {
        return error -> {
            try {
                nacknowledger.accept(error);
            } finally {
                span.setTag(Tags.ERROR, true);
                span.log(createErrorFields(error));
                span.finish();
            }
        };
    }

    private static Map<String, Object> createErrorFields(Throwable error) {
        Map<String, Object> fields = new HashMap<>();
        fields.put(Fields.EVENT, "error");
        fields.put(Fields.ERROR_KIND, error.getClass().getSimpleName());
        fields.put(Fields.ERROR_OBJECT, error);
        return fields;
    }

    /**
     * This extension is necessary to make one-to-many mappings work correctly. The underlying span
     * is propagated to all downstream mappings such that it can be activated in context. However,
     * none of those mappings should finish the span. This is in contrast to "metering" where we
     * do not propagate the meters at all, and simply rely on the originating element's
     * acknowledgement to eventually be executed.
     */
    private static final class Propagated<T> extends TracingAlo<T> {

        public Propagated(Alo<T> delegate, TracerFacade tracerFacade, Span span) {
            super(delegate, tracerFacade, span);
        }

        @Override
        public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
            Alo<R> result = tracerFacade.map(span, mapper, delegate::map);
            return new Propagated<>(result, tracerFacade, span);
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
