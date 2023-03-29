package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.core.AloDecorator;
import io.micrometer.core.instrument.Tags;

/**
 * Templated implementation of {@link Alo} metering decoration
 *
 * @param <T> The type of data referenced by decorated {@link Alo}s
 */
public abstract class MeteringAloDecorator<T> implements AloDecorator<T> {

    private final MeterFacade meterFacade = MeterFacade.global();

    @Override
    public final Alo<T> decorate(Alo<T> alo) {
        Tags tags = extractTags(alo.get());
        return MeteringAlo.start(alo, meterFacade, tags);
    }

    /**
     * Extract base set of {@link io.micrometer.core.instrument.Tag}s for metrics. Should at least
     * include a {@code type} Tag describing the type of the {@link Alo} payload being metered.
     *
     * @param t The type of items referenced by decorated {@link Alo}s
     * @return Base set of tags to be applied to all exported metrics
     */
    protected abstract Tags extractTags(T t);
}
