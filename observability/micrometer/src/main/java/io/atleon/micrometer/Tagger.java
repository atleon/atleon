package io.atleon.micrometer;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.function.Function;

/**
 * Interface through which {@link Tag}s for a specific type can be extracted and used to identify
 * {@link io.micrometer.core.instrument.Meter}s
 *
 * @param <T> The type from which {@link Tag}s are extracted
 */
@FunctionalInterface
public interface Tagger<T> {

    /**
     * Creates a simple tagger composed of the provided base Tags and function that extracts more
     * Tags from available values of the targeted type.
     *
     * @param base      Set of tags that are used for all Meters
     * @param extractor Function to extract more tags when instance of targeted type is available
     * @param <T>       The target type to extract tags from, when an instance is available
     * @return A new Tagger
     */
    static <T> Tagger<T> composed(Iterable<Tag> base, Function<? super T, ? extends Iterable<Tag>> extractor) {
        return new Composed<>(base, extractor);
    }

    default Iterable<Tag> base() {
        return Tags.empty();
    }

    Iterable<Tag> extract(T t);

    class Composed<T> implements Tagger<T> {

        private final Iterable<Tag> base;

        private final Function<? super T, ? extends Iterable<Tag>> extractor;

        private Composed(Iterable<Tag> base, Function<? super T, ? extends Iterable<Tag>> extractor) {
            this.base = base;
            this.extractor = extractor;
        }

        @Override
        public Iterable<Tag> base() {
            return base;
        }

        @Override
        public Iterable<Tag> extract(T t) {
            return extractor.apply(t);
        }
    }
}
