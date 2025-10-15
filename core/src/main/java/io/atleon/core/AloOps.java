package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.ContextView;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Common utility methods associated with operations on Alo and related components
 */
final class AloOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(AloOps.class);

    private AloOps() {

    }

    public static <T> BiConsumer<Alo<T>, SynchronousSink<Alo<T>>>
    filteringHandler(Predicate<? super T> predicate, Consumer<? super Alo<T>> negativeConsumer) {
        return (alo, sink) -> {
            Boolean result = null;
            try {
                result = alo.supplyInContext(() -> predicate.test(alo.get()));
            } catch (Throwable error) {
                processFailureOrNacknowledge(sink, alo, error);
            }

            if (result != null) {
                if (result) {
                    sink.next(alo);
                } else {
                    handleDiscard(sink.contextView(), alo, negativeConsumer);
                }
            }
        };
    }

    public static <T, R> BiConsumer<Alo<T>, SynchronousSink<Alo<R>>>
    typeFilteringHandler(Class<R> clazz, Consumer<? super Alo<T>> negativeConsumer) {
        return (alo, sink) -> {
            if (clazz.isAssignableFrom(alo.get().getClass())) {
                sink.next((Alo<R>) alo);
            } else {
                handleDiscard(sink.contextView(), alo, negativeConsumer);
            }
        };
    }

    public static <T, R> BiConsumer<Alo<T>, SynchronousSink<Alo<R>>>
    mappingHandler(Function<? super T, ? extends R> mapper) {
        return (alo, sink) -> {
            Alo<R> result = null;
            try {
                // Note: mapping is not invoked with *InContext since we're delegating to Alo anyway
                result = Objects.requireNonNull(alo.map(mapper), "Alo implementation returned null mapping");
            } catch (Throwable error) {
                processFailureOrNacknowledge(sink, alo, error);
            }

            if (result != null) {
                sink.next(result);
            }
        };
    }

    public static <T, R> BiConsumer<Alo<T>, SynchronousSink<Alo<R>>>
    mappingPresentHandler(Function<? super T, Optional<? extends R>> mapper, Consumer<? super Alo<T>> absentConsumer) {
        return (alo, sink) -> {
            Alo<Optional<? extends R>> result = null;
            try {
                // Note: mapping is not invoked with *InContext since we're delegating to Alo anyway
                result = Objects.requireNonNull(alo.map(mapper), "Alo implementation returned null mapping");
            } catch (Throwable error) {
                processFailureOrNacknowledge(sink, alo, error);
            }

            if (result != null) {
                if (result.get().isPresent()) {
                    sink.next(PresentAlo.wrap(result));
                } else {
                    absentConsumer.accept(alo);
                }
            }
        };
    }

    public static <T> BiConsumer<Alo<T>, SynchronousSink<Alo<Void>>>
    consumingHandler(Consumer<? super T> consumer, Consumer<? super Alo<T>> afterSuccessConsumer) {
        return (alo, sink) -> {
            boolean consumed = false;
            try {
                alo.runInContext(() -> consumer.accept(alo.get()));
                consumed = true;
            } catch (Throwable error) {
                processFailureOrNacknowledge(sink, alo, error);
            }

            if (consumed) {
                afterSuccessConsumer.accept(alo);
            }
        };
    }

    public static <T> BiConsumer<Alo<T>, SynchronousSink<Alo<T>>>
    failureProcessingHandler(Predicate<? super T> isFailure, Function<? super T, ? extends Throwable> errorExtractor) {
        return (alo, sink) -> {
            T t = alo.get();
            if (isFailure.test(t)) {
                processFailure(sink, alo, errorExtractor.apply(t), () -> sink.next(alo));
            } else {
                sink.next(alo);
            }
        };
    }

    public static <T> UnaryOperator<Alo<T>> acknowledgerDecorator(Consumer<? super T> decorator) {
        return alo -> {
            T t = alo.get();
            Runnable decoratedAcknowledger = combineRunnables(() -> decorator.accept(t), alo.getAcknowledger());
            return alo.<T>propagator().create(t, decoratedAcknowledger, alo.getNacknowledger());
        };
    }

    public static <T> Alo<List<T>> fanIn(List<Alo<T>> alos) {
        Alo<T> firstAlo = alos.get(0);
        if (alos.size() == 1) {
            return firstAlo.map(Collections::singletonList);
        } else {
            return firstAlo.<T>fanInPropagator(alos).create(
                alos.stream().map(Alo::get).collect(Collectors.toList()),
                combineAcknowledgers(alos.stream().map(Alo::getAcknowledger).collect(Collectors.toList())),
                combineNacknowledgers(alos.stream().map(Alo::getNacknowledger).collect(Collectors.toList()))
            );
        }
    }

    private static void processFailureOrNacknowledge(SynchronousSink<?> sink, Alo<?> alo, Throwable error) {
        processFailure(sink, alo, error, () -> Alo.nacknowledge(alo, error));
    }

    private static void processFailure(SynchronousSink<?> sink, Alo<?> alo, Throwable error, Runnable unprocessedFallback) {
        if (!AloFailureStrategy.choose(sink).process(alo, error, sink::error)) {
            unprocessedFallback.run();
        }
    }

    private static <T> void handleDiscard(ContextView contextView, Alo<T> alo, Consumer<? super Alo<T>> afterHandle) {
        try {
            alo.runInContext(() -> DiscardHook.choose(contextView).accept(alo.get()));
        } catch (Throwable error) {
            LOGGER.warn("Error in discard hook", error);
        } finally {
            afterHandle.accept(alo);
        }
    }

    private static Runnable combineAcknowledgers(Iterable<? extends Runnable> acknowledgers) {
        return () -> acknowledgers.forEach(Runnable::run);
    }

    private static Consumer<? super Throwable>
    combineNacknowledgers(Iterable<? extends Consumer<? super Throwable>> nacknowledgers) {
        return error -> nacknowledgers.forEach(nacknowledger -> nacknowledger.accept(error));
    }

    private static Runnable combineRunnables(Runnable first, Runnable second) {
        return () -> {
            first.run();
            second.run();
        };
    }
}
