package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.core.AloSignalObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import reactor.core.publisher.Signal;
import reactor.core.publisher.SignalType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * Templated implementation of metering on {@link Signal}s referencing {@link Alo} values
 *
 * @param <T> The type of data exposed by Alo items in emitted Signal values
 */
public abstract class MeteringAloSignalObserver<T, K> implements AloSignalObserver<T> {

    private final MeterFacade<TypeKey<SignalType, K>> meterFacade;

    protected MeteringAloSignalObserver(String name) {
        this(Metrics.globalRegistry, name);
    }

    protected MeteringAloSignalObserver(MeterRegistry meterRegistry, String name) {
        this.meterFacade = MeterFacade.create(meterRegistry, it -> new MeterKey(name, toTags(it)));
    }

    /**
     * Creates a simple metering Observer with the provided name and optional set of statically
     * provided key-value tags.
     *
     * @param name             Name to be used for all created Meters
     * @param baseTagKeyValues Optional set of key-value tags to add to each Meter
     * @param <T>              The type of data exposed by Alo items in emitted Signal values
     * @return A new metering {@link AloSignalObserver}
     */
    public static <T> AloSignalObserver<T> simple(String name, String... baseTagKeyValues) {
        return new Composed<>(name, __ -> Void.class, Tagger.composed(Tags.of(baseTagKeyValues), it -> Tags.empty()));
    }

    /**
     * Creates a simple metering Observer with the provided name and {@link Function}, which
     * extracts an arbitrary String value from data exposed by observed Alo items as a "type" tag.
     * Note that the extracted type should be low in cardinality such as to provide a meaningful
     * delineation of metrics and not explode the number of created meters.
     *
     * @param name          Name to be used for all created Meters
     * @param typeExtractor Function that extracts a
     * @param <T>           The type of data exposed by Alo items in emitted Signal values
     * @return A new metering {@link AloSignalObserver}
     */
    public static <T> AloSignalObserver<T> extractType(String name, Function<? super T, String> typeExtractor) {
        return new Composed<>(name, typeExtractor, type -> Tags.of("type", type));
    }

    /**
     * Creates a metering Observer with the provided name, key extractor Function, and
     * {@link Tagger}. The key extractor function is used to identify created meters, and its
     * cardinality is directly correlated to the number of created meters. The tagger is used to
     * both extract a base set of tags used on all meters, as well as extract extra tags when a
     * value is associated with observed {@link Signal}s (i.e. onNext)
     *
     * @param name         Name to be used for all created Meters
     * @param keyExtractor {@link Function} used to extract identifying meter information
     * @param tagger       Implementation of {@link Tagger} to extract tags for all meters
     * @param <T>          The type of data exposed by Alo items in emitted Signal values
     * @param <K>          The type of key used to identify/delineate meters
     * @return A new metering {@link AloSignalObserver}
     */
    public static <T, K> AloSignalObserver<T>
    composed(String name, Function<? super T, K> keyExtractor, Tagger<? super K> tagger) {
        return new Composed<>(name, keyExtractor, tagger);
    }

    @Override
    public void configure(Map<String, ?> properties) {
        meterFacade.clear();
    }

    @Override
    public void accept(Signal<Alo<T>> signal) {
        switch (signal.getType()) {
            case REQUEST:
            case CANCEL:
            case ON_NEXT:
            case ON_ERROR:
                TypeKey<SignalType, K> typeKey = signal.hasValue()
                    ? new TypeKey<>(signal.getType(), extractKey(signal.get().get()))
                    : new TypeKey<>(signal.getType());
                meterFacade.counter(typeKey).increment();
        }
    }

    /**
     * Extracts a key that is used to identify {@link io.micrometer.core.instrument.Meter}s used to
     * decorate {@link Alo} processing. The cardinality of the key directly correlates with the
     * cardinality of created Meters.
     *
     * @param value Instance of data referenced by decorated {@link Alo} element
     * @return A key used as part of the identifier for created metrics
     */
    protected abstract K extractKey(T value);

    protected final Tags toTags(TypeKey<SignalType, K> typeKey) {
        Collection<Tag> tags = new ArrayList<>();
        tags.add(Tag.of("signal_type", typeKey.typeName()));
        baseTags().forEach(tags::add);
        typeKey.mapKey(this::extractTags).ifPresent(extractedTags -> extractedTags.forEach(tags::add));
        return Tags.of(tags);
    }

    /**
     * @return Base set of tags to be applied to all {@link Signal} Meters
     */
    protected abstract Iterable<Tag> baseTags();

    /**
     * Extracts extra tags to be applied when there is a value associated with a metered
     * {@link Signal} (i.e. onNext). The provided key is what was extracted by the implementation
     * from the {@link Alo} value contained in the originating Signal.
     *
     * @param key The Meter identifier extracted from emitted Signal value
     * @return Extra tags to be applied to corresponding Signal's Meter
     */
    protected abstract Iterable<Tag> extractTags(K key);

    private static final class Composed<T, K> extends MeteringAloSignalObserver<T, K> {

        private final Function<? super T, K> keyExtractor;

        private final Tagger<? super K> tagger;

        public Composed(String name, Function<? super T, K> keyExtractor, Tagger<? super K> tagger) {
            super(name);
            this.keyExtractor = keyExtractor;
            this.tagger = tagger;
        }

        @Override
        protected K extractKey(T value) {
            return keyExtractor.apply(value);
        }

        @Override
        protected Iterable<Tag> baseTags() {
            return tagger.base();
        }

        @Override
        protected Iterable<Tag> extractTags(K key) {
            return tagger.extract(key);
        }
    }
}
