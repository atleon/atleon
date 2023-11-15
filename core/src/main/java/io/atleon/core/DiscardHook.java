package io.atleon.core;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A hook that may be applied when elements from a pipeline are discarded, e.g. via filtering
 */
interface DiscardHook extends Consumer<Object> {

    /**
     * Creates a function that will prepend the provided hook to hooks applied later in a pipeline
     */
    static <R> Function<Context, Context> newContextModifier(Class<R> type, Consumer<? super R> hook) {
        DiscardHook discardHook = object -> {
            if (type.isInstance(object)) {
                hook.accept(type.cast(object));
            }
        };

        return context -> {
            DiscardHook contextualHook = context.getOrDefault(DiscardHook.class, null);
            return context.put(DiscardHook.class, contextualHook == null ? discardHook : discardHook.andThen(contextualHook));
        };
    }

    static DiscardHook choose(ContextView contextView) {
        return contextView.getOrDefault(DiscardHook.class, DiscardHook.noOp());
    }

    static DiscardHook noOp() {
        return __ -> {

        };
    }

    @Override
    default DiscardHook andThen(Consumer<? super Object> after) {
        return object -> {
            accept(object);
            after.accept(object);
        };
    }
}
