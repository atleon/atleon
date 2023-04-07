package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.core.AloSignalListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import reactor.core.publisher.Signal;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Templated implementation of metering on {@link Signal}s referencing {@link Alo} values
 *
 * @param <T> The type of data exposed by Alo items in emitted Signal values
 */
public abstract class MeteringAloSignalListener<T> implements AloSignalListener<T> {

    private final MeterFacade meterFacade;

    private final String name;

    protected MeteringAloSignalListener(String name) {
        this(Metrics.globalRegistry, name);
    }

    protected MeteringAloSignalListener(MeterRegistry meterRegistry, String name) {
        this.meterFacade = MeterFacade.wrap(meterRegistry);
        this.name = name;
    }

    public static <T> AloSignalListener<T> composed(String name, String... tagKeyValues) {
        return new Composed<>(name, Tags.of(tagKeyValues), value -> Tags.empty());
    }

    public static <T> AloSignalListener<T> composed(String name, Function<? super T, Iterable<Tag>> valueTagsExtractor) {
        return new Composed(name, Tags.empty(), valueTagsExtractor);
    }

    public static <T> AloSignalListener<T>
    composed(String name, Iterable<Tag> baseTags, Function<? super T, Iterable<Tag>> valueTagsExtractor) {
        return new Composed<>(name, Tags.of(baseTags), valueTagsExtractor);
    }

    @Override
    public void accept(Signal<Alo<T>> signal) {
        switch (signal.getType()) {
            case CANCEL:
            case ON_NEXT:
            case ON_ERROR:
                meterFacade.counter(name, extractTags(signal)).increment();
        }
    }

    protected final Tags extractTags(Signal<Alo<T>> signal) {
        List<Tag> tags = new ArrayList<>();
        baseTags().forEach(tags::add);
        if (signal.hasValue()) {
            extractValueTags(signal.get().get()).forEach(tags::add);
        }
        tags.add(Tag.of("signalType", signal.getType().name()));
        return Tags.of(tags);
    }

    protected abstract Iterable<Tag> baseTags();

    protected Iterable<Tag> extractValueTags(T value) {
        return Tags.empty();
    }

    private static final class Composed<T> extends MeteringAloSignalListener<T> {

        private final Tags baseTags;

        private final Function<? super T, Iterable<Tag>> valueTagsExtractor;

        public Composed(String name, Tags baseTags, Function<? super T, Iterable<Tag>> valueTagsExtractor) {
            super(name);
            this.baseTags = baseTags;
            this.valueTagsExtractor = valueTagsExtractor;
        }

        @Override
        protected Iterable<Tag> baseTags() {
            return baseTags;
        }

        @Override
        protected Iterable<Tag> extractValueTags(T value) {
            return valueTagsExtractor.apply(value);
        }
    }
}
