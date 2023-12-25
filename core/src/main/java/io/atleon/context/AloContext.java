package io.atleon.context;

import io.atleon.core.Alo;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * Context that is scoped to {@link Alo} elements. Management of this context is activated through
 * the {@link ContextActivatingAlo} decorator. When activated, operations on Alo elements may
 * access the active context associated with a given Alo via {@link AloContext#active()}.
 * <p>
 * Values set in AloContext are <i>stable</i>. Once set, they cannot be changed.
 * <p>
 * The intent of AloContext is to store diagnostic-oriented metadata. For example, one might store
 * the {@link java.time.Instant Instant} at which a calculation is performed in order to provide
 * fine-grained process timing metrics.
 * <p>
 * It is <i>not</i> intended for AloContext to store "a lot" of elements. "A lot" in this case
 * would be more than 10.
 * <p>
 * There's a little bit of nuance concerned with how contexts are propagated:
 * <ul>
 *     <li>1-to-1 mapping (i.e. map): Context propagated as-is</li>
 *     <li>1-to-many mapping (i.e. flatMapIterable, flatMap): Context copied and propagated</li>
 *     <li>many-to-1 mapping (i.e. bufferTimeout): Contexts merged to new context using reduction</li>
 * </ul>
 * The reduction used in many-to-1 mappings is based on the {@link Key} instance, which provides a
 * reducing {@link BinaryOperator}.
 */
public abstract class AloContext {

    private static final AloContext NO_OP = new NoOp();

    private static final ThreadLocal<AloContext> ACTIVE_CONTEXT = new ThreadLocal<>();

    private AloContext() {

    }

    public static AloContext active() {
        AloContext context = ACTIVE_CONTEXT.get();
        return context == null ? NO_OP : context;
    }

    /**
     * Returns the value set for the given key, or empty if not set.
     */
    public abstract <T> Optional<T> get(Key<T> key);

    /**
     * Sets the value for the given key, and returns whether the value was successfully updated.
     */
    public abstract <T> boolean set(Key<T> key, T value);

    static AloContext create() {
        return new Impl();
    }

    final void run(Runnable runnable) {
        AloContext previousContext = ACTIVE_CONTEXT.get();
        try {
            ACTIVE_CONTEXT.set(this);
            runnable.run();
        } finally {
            ACTIVE_CONTEXT.set(previousContext);
        }
    }

    final <T> T supply(Supplier<T> supplier) {
        AloContext previousContext = ACTIVE_CONTEXT.get();
        try {
            ACTIVE_CONTEXT.set(this);
            return supplier.get();
        } finally {
            ACTIVE_CONTEXT.set(previousContext);
        }
    }

    abstract AloContext copy();

    abstract void merge(AloContext other);

    abstract void forEach(BiConsumer<Key<?>, Object> consumer);

    /**
     * Key for indexing values stored in an {@link AloContext}. Keys use reference equality and
     * AloContext does not provide a (public) mechanism to loop over Keys. This means there is no
     * way to access a Key's value from an AloContext without having access to the Key instance
     * itself. This allows strong control over what code can find/set a key in the AloContext.
     * Generally, Keys should be stored in static fields.
     * <p>
     * Keys are constructed with a {@link BinaryOperator} that is used in many-to-one mappings in
     * order to create merged contexts. See the available static constructors for the available
     * reductions.
     *
     * @param <T> The type of value that is indexed by this Key
     */
    public static final class Key<T> {

        private final String name;

        private final BinaryOperator<T> reducer;

        private Key(String name, BinaryOperator<T> reducer) {
            this.name = name;
            this.reducer = reducer;
        }

        /**
         * Creates a new Key with the given name to index a given type of value. If/when
         * AloContexts are merged, the value used in the resulting AloContext is the first-merged
         * value. This is useful and performant when it is likely that all merged values are the
         * same, or if it is unimportant what value results from merging.
         */
        public static <T> Key<T> single(String name) {
            return new Key<>(name, (previous, next) -> previous);
        }

        /**
         * Creates a new Key with the given name to index a given type of {@link Comparable} value.
         * If/when AloContexts are merged, the value used in the resulting AloContext is the
         * minimum value based on {@link Comparable#compareTo(Object)}.
         */
        public static <T extends Comparable<? super T>> Key<T> min(String name) {
            return min(name, Comparator.<T>naturalOrder());
        }

        /**
         * Creates a new Key with the given name to index a given type of value. If/when
         * AloContexts are merged, the value used in the resulting AloContext is the minimum value
         * based on the provided {@link Comparator}.
         */
        public static <T> Key<T> min(String name, Comparator<T> comparator) {
            return new Key<>(name, (v1, v2) -> comparator.compare(v1, v2) <= 0 ? v1 : v2);
        }

        @Override
        public String toString() {
            return name;
        }

        T reduce(T previous, T next) {
            return reducer.apply(previous, next);
        }
    }

    private static final class NoOp extends AloContext {

        @Override
        public <T> Optional<T> get(Key<T> key) {
            return Optional.empty();
        }

        @Override
        public <T> boolean set(Key<T> key, T value) {
            return false;
        }

        @Override
        AloContext copy() {
            return this;
        }

        @Override
        void merge(AloContext other) {

        }

        @Override
        void forEach(BiConsumer<Key<?>, Object> consumer) {

        }
    }

    private static final class Impl extends AloContext {

        private final Map<Key<?>, Object> map = new ConcurrentHashMap<>();

        @Override
        public <T> Optional<T> get(Key<T> key) {
            return Optional.ofNullable((T) map.get(key));
        }

        @Override
        public <T> boolean set(Key<T> key, T value) {
            return map.putIfAbsent(key, value) == null;
        }

        @Override
        AloContext copy() {
            Impl copy = new Impl();
            copy.map.putAll(map);
            return copy;
        }

        @Override
        void merge(AloContext other) {
            other.forEach((key, value) -> map.compute(key, (__, previous) -> remap((Key<Object>) key, previous, value)));
        }

        @Override
        void forEach(BiConsumer<Key<?>, Object> consumer) {
            map.forEach(consumer);
        }

        private static <T> T remap(Key<T> key, T previous, T next) {
            return previous == null ? next : key.reduce(previous, next);
        }
    }
}
