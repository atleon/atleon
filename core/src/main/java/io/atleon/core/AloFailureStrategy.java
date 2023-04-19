package io.atleon.core;

import reactor.core.publisher.SynchronousSink;

import java.util.function.Predicate;

/**
 * A strategy applied when operations on {@link Alo} elements fail. When processing an error
 * succeeds (strategy returns true), the calling context is safe to discard both the error and
 * incriminating {@link Alo}, then continue processing. When processing an error fails (strategy
 * returns false), the calling context must either negatively acknowledge the {@link Alo} element
 * or otherwise forward the/an error-containing element in the pipeline.
 */
interface AloFailureStrategy {

    AloFailureStrategy EMIT = new Emit(__ -> true);

    AloFailureStrategy DELEGATE = new Delegate(__ -> true);

    /**
     * Process the error and the {@link Alo} that caused it.
     * <p>
     * If the strategy fully processes the error, potentially by calling
     * {@link SynchronousSink#error(Throwable)} or {@link Alo#acknowledge(Alo)}, this method must
     * return true. When the method returns false, handling the error is delegated to the caller,
     * who may choose to call {@link Alo#nacknowledge(Alo, Throwable)} or otherwise forward the
     * error in the pipeline.
     *
     * @param sink  Sink into which the provided error may be emitted
     * @param alo   The Alo that caused the error
     * @param error The error that has been encountered
     * @return Whether the error has been fully handled
     */
    boolean process(SynchronousSink<?> sink, Alo<?> alo, Throwable error);

    static AloFailureStrategy emit() {
        return EMIT;
    }

    static AloFailureStrategy emitUnless(Predicate<? super Throwable> errorPredicate) {
        return new Emit(errorPredicate.negate());
    }

    static AloFailureStrategy delegate() {
        return DELEGATE;
    }

    static AloFailureStrategy delegateUnless(Predicate<? super Throwable> errorPredicate) {
        return new Delegate(errorPredicate.negate());
    }

    class Emit implements AloFailureStrategy {

        private final Predicate<? super Throwable> errorPredicate;

        Emit(Predicate<? super Throwable> errorPredicate) {
            this.errorPredicate = errorPredicate;
        }

        @Override
        public boolean process(SynchronousSink<?> sink, Alo<?> alo, Throwable error) {
            if (errorPredicate.test(error)) {
                sink.error(error);
            } else {
                Alo.acknowledge(alo);
            }
            return true;
        }
    }

    class Delegate implements AloFailureStrategy {

        private final Predicate<? super Throwable> errorPredicate;

        Delegate(Predicate<? super Throwable> errorPredicate) {
            this.errorPredicate = errorPredicate;
        }

        @Override
        public boolean process(SynchronousSink<?> sink, Alo<?> alo, Throwable error) {
            if (errorPredicate.test(error)) {
                return false;
            } else {
                Alo.acknowledge(alo);
                return true;
            }
        }
    }
}
