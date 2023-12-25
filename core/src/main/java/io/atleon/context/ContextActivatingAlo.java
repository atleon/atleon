package io.atleon.context;

import io.atleon.core.AbstractDecoratingAlo;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Decorates {@link Alo} elements with {@link AloContext} activation and propagation
 *
 * @param <T>
 */
public class ContextActivatingAlo<T> extends AbstractDecoratingAlo<T> {

    private final AloContext context;

    private ContextActivatingAlo(Alo<T> delegate, AloContext context) {
        super(delegate);
        this.context = context;
    }

    public static <T> ContextActivatingAlo<T> create(Alo<T> delegate) {
        return new ContextActivatingAlo<>(delegate, AloContext.create());
    }

    @Override
    public void runInContext(Runnable runnable) {
        context.run(() -> delegate.runInContext(runnable));
    }

    @Override
    public <R> R supplyInContext(Supplier<R> supplier) {
        return context.supply(() -> delegate.supplyInContext(supplier));
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return new ContextActivatingAlo<>(context.supply(() -> delegate.map(mapper)), context);
    }

    @Override
    public <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos) {
        AloContext mergedContext = AloContext.create();
        for (Alo<?> alo : alos) {
            doOnDelegator(alo, ContextActivatingAlo.class, it -> mergedContext.merge(it.context()));
        }
        return delegate.<R>fanInPropagator(alos).withDecorator(alo -> new ContextActivatingAlo<>(alo, mergedContext));
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return delegate.<R>propagator().withDecorator(alo -> new ContextActivatingAlo<>(alo, context.copy()));
    }

    public AloContext context() {
        return context;
    }
}
