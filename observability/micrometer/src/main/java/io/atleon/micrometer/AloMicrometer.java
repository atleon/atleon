package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.micrometer.core.instrument.Tags;
import reactor.core.observability.SignalListenerFactory;

import java.util.function.Function;

/**
 * Utilities for creating Atleon-specific {@link SignalListenerFactory} instances that meter
 * reactive signals in pipelines of {@link Alo} elements through Micrometer.
 */
public final class AloMicrometer {

    private AloMicrometer() {

    }

    /**
     * Creates a simple metering {@link SignalListenerFactory} for {@link Alo} items with the
     * provided name and optional set of statically provided key-value tags.
     *
     * @param name             Name to be used for all created meters
     * @param baseTagKeyValues Optional set of key-value tags to add to each meter
     * @param <T>              The type of data exposed by Alo items in reactive signals (i.e. onNext)
     * @return A new metering {@link SignalListenerFactory} for reactive pipelines of Alo
     */
    public static <T> SignalListenerFactory<Alo<T>, ?> metrics(String name, String... baseTagKeyValues) {
        return metrics(name, __ -> null, Tagger.composed(Tags.of(baseTagKeyValues), __ -> Tags.empty()));
    }

    /**
     * Creates a metering {@link SignalListenerFactory} for {@link Alo} items with the provided
     * name, String-valued key extractor function, and a tag name used to incorporate extracted
     * keys as tag values on created meters. Note that the cardinality of the key extractor
     * function is directly correlated to the number of created meters.
     *
     * @param name               Name to be used for all created meters
     * @param stringKeyExtractor {@link Function} used to extract String-valued identifying meter information
     * @param keyTagName         Name of the tag used to identify extracted key values
     * @param <T>                The type of data exposed by Alo items in emitted Signal values
     * @return A new metering {@link SignalListenerFactory} for reactive pipelines of Alo
     */
    public static <T> SignalListenerFactory<Alo<T>, ?>
    metrics(String name, Function<? super T, String> stringKeyExtractor, String keyTagName) {
        return metrics(name, stringKeyExtractor, Tagger.composed(Tags.empty(), key -> Tags.of(keyTagName, key)));
    }

    /**
     * Creates a metering {@link SignalListenerFactory} for {@link Alo} items with the provided
     * name, key extractor function, and {@link Tagger}. The key extractor function is used to
     * identify created meters, and its cardinality is directly correlated to the number of created
     * meters. The tagger is used to both extract a base set of tags used on all meters, as well as
     * extract extra tags when a value is associated with reactive signals (i.e. onNext).
     *
     * @param name         Name to be used for all created meters
     * @param keyExtractor {@link Function} used to extract identifying meter information
     * @param tagger       Implementation of {@link Tagger} to extract tags for all meters
     * @param <T>          The type of data exposed by Alo items in emitted Signal values
     * @param <K>          The type of key used to identify/delineate meters
     * @return A new metering {@link SignalListenerFactory} for reactive pipelines of Alo
     */
    public static <T, K> SignalListenerFactory<Alo<T>, ?>
    metrics(String name, Function<? super T, K> keyExtractor, Tagger<? super K> tagger) {
        return new MeteringAloSignalListenerFactory<T, K>(name) {

            @Override
            protected Function<? super T, K> keyExtractor() {
                return keyExtractor;
            }

            @Override
            protected Tagger<? super K> tagger() {
                return tagger;
            }
        };
    }
}
