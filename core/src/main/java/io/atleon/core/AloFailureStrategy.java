package io.atleon.core;

import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A strategy applied when operations on {@link Alo} elements fail. When processing an error
 * succeeds (strategy returns true), the calling context is safe to discard both the error and
 * incriminating {@link Alo}, then continue processing. When processing an error fails (strategy
 * returns false), the calling context must either negatively acknowledge the {@link Alo} element
 * or otherwise forward the/an error-containing element in the pipeline.
 */
interface AloFailureStrategy {

    AloFailureStrategy EMIT = new Emit((__, error) -> true);

    AloFailureStrategy DELEGATE = new Delegate((__, error) -> true);

    /**
     * Choose an {@link AloFailureStrategy} based on the provided {@link Subscriber}. This is done
     * by introspecting the provided Subscriber to extract contextual information indicating what
     * strategy to use.
     *
     * @param subscriber A {@link Subscriber} to introspect for an {@link AloFailureStrategy}
     * @return An {@link AloFailureStrategy} to be used to process failures
     */
    static AloFailureStrategy choose(Subscriber<?> subscriber) {
        if (subscriber instanceof CoreSubscriber) {
            return choose(CoreSubscriber.class.cast(subscriber).currentContext());
        } else {
            return choose(Context.empty());
        }
    }

    /**
     * Choose an {@link AloFailureStrategy} based on the provided {@link SynchronousSink}. This is
     * done using the sink's {@link ContextView} to choose the strategy to apply.
     *
     * @param sink A {@link SynchronousSink} whose context will be used to choose a strategy
     * @return An {@link AloFailureStrategy} to be used to process failures
     */
    static AloFailureStrategy choose(SynchronousSink<?> sink) {
        return choose(sink.contextView());
    }

    /**
     * Choose an {@link AloFailureStrategy} based on the provided {@link ContextView}.
     *
     * @param contextView A {@link ContextView} that may contain an explicit strategy to use
     * @return An {@link AloFailureStrategy} to be used to process failures
     */
    static AloFailureStrategy choose(ContextView contextView) {
        return contextView.getOrDefault(AloFailureStrategy.class, AloFailureStrategy.emit());
    }

    /**
     * Process the error and the {@link Alo} that caused it.
     * <p>
     * If the strategy fully processes the error, potentially by calling {@link Alo#acknowledge(Alo)}
     * on the provided Alo or {@link Consumer#accept(Object)} on the provided error emitter, the
     * strategy *must* return true. When the method returns false, handling the error is delegated
     * to the caller, who may choose to call {@link Alo#nacknowledge(Alo, Throwable)} or otherwise
     * forward the error in the pipeline.
     *
     * @param alo          The Alo that caused the error
     * @param error        The error that has been encountered
     * @param errorEmitter Consumer into which the provided error may be emitted
     * @return Whether the error has been fully handled
     */
    boolean process(Alo<?> alo, Throwable error, Consumer<Throwable> errorEmitter);

    static AloFailureStrategy emit() {
        return EMIT;
    }

    static AloFailureStrategy emit(BiPredicate<Object, ? super Throwable> errorPredicate) {
        return new Emit(errorPredicate);
    }

    static AloFailureStrategy delegate() {
        return DELEGATE;
    }

    static AloFailureStrategy delegate(BiPredicate<Object, ? super Throwable> errorPredicate) {
        return new Delegate(errorPredicate);
    }

    class Emit implements AloFailureStrategy {

        private final BiPredicate<Object, ? super Throwable> errorPredicate;

        private Emit(BiPredicate<Object, ? super Throwable> errorPredicate) {
            this.errorPredicate = errorPredicate;
        }

        @Override
        public boolean process(Alo<?> alo, Throwable error, Consumer<Throwable> errorEmitter) {
            if (alo.supplyInContext(() -> errorPredicate.test(alo.get(), error))) {
                errorEmitter.accept(error);
            } else {
                Alo.acknowledge(alo);
            }
            return true;
        }
    }

    class Delegate implements AloFailureStrategy {

        private final BiPredicate<Object, ? super Throwable> errorPredicate;

        private Delegate(BiPredicate<Object, ? super Throwable> errorPredicate) {
            this.errorPredicate = errorPredicate;
        }

        @Override
        public boolean process(Alo<?> alo, Throwable error, Consumer<Throwable> errorEmitter) {
            if (alo.supplyInContext(() -> errorPredicate.test(alo.get(), error))) {
                return false;
            } else {
                Alo.acknowledge(alo);
                return true;
            }
        }
    }
}
