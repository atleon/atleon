package io.atleon.micrometer.observation;

import io.atleon.core.Alo;
import io.atleon.core.AloDecorator;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.Observations;
import io.micrometer.observation.docs.ObservationDocumentation;
import org.jspecify.annotations.Nullable;

/**
 * Templated implementation of {@link Alo} observing decoration for consumed data.
 *
 * @param <T> The type of data being consumed
 */
public abstract class ObservingAloConsumptionDecorator<T, C extends Observation.Context> implements AloDecorator<T> {

    private final ObservationDocumentation documentation;

    private final ObservationRegistry registry;

    protected ObservingAloConsumptionDecorator(ObservationDocumentation documentation) {
        this(documentation, Observations.getGlobalRegistry());
    }

    protected ObservingAloConsumptionDecorator(ObservationDocumentation documentation, ObservationRegistry registry) {
        this.documentation = documentation;
        this.registry = registry;
    }

    @Override
    public int order() {
        return INNERMOST_ORDER + 5000;
    }

    @Override
    public final Alo<T> decorate(Alo<T> alo) {
        Observation observation = documentation.observation(
                customConvention(), defaultConvention(), () -> createContext(alo.get()), registry);
        return ObservingAlo.start(alo, registry, observation);
    }

    protected @Nullable ObservationConvention<C> customConvention() {
        return null;
    }

    protected abstract ObservationConvention<C> defaultConvention();

    protected abstract C createContext(T t);
}
