package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.core.AloSignalListenerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.observability.DefaultSignalListener;
import reactor.core.observability.SignalListener;
import reactor.core.publisher.SignalType;
import reactor.util.context.ContextView;

/**
 * Templated implementation of {@link reactor.core.observability.SignalListenerFactory} that
 * creates {@link SignalListener} instances which apply metering to Reactor
 * Publishers of {@link io.atleon.core.Alo} items.
 *
 * @param <T> The type of data exposed by Alo values in emitted onNext signals
 */
public abstract class MeteringAloSignalListenerFactory<T, K> implements AloSignalListenerFactory<T, Void> {

    private final MeterRegistry meterRegistry;

    private final String name;

    protected MeteringAloSignalListenerFactory(String name) {
        this(Metrics.globalRegistry, name);
    }

    protected MeteringAloSignalListenerFactory(MeterRegistry meterRegistry, String name) {
        this.meterRegistry = meterRegistry;
        this.name = name;
    }

    @Override
    public Void initializePublisherState(Publisher<? extends Alo<T>> source) {
        return null;
    }

    @Override
    public SignalListener<Alo<T>> createListener(
            Publisher<? extends Alo<T>> source, ContextView listenerContext, Void publisherContext) {
        return new MeteringAloSignalListener<>(meterRegistry, name, keyExtractor(), tagger());
    }

    protected abstract Function<? super T, K> keyExtractor();

    protected abstract Tagger<? super K> tagger();

    private static final class MeteringAloSignalListener<T, K> extends DefaultSignalListener<Alo<T>> {

        private final MeterFacade<TypeKey<SignalType, K>> meterFacade;

        private final Function<? super T, K> keyExtractor;

        private final Tagger<? super K> tagger;

        public MeteringAloSignalListener(
                MeterRegistry meterRegistry,
                String name,
                Function<? super T, K> keyExtractor,
                Tagger<? super K> tagger) {
            this.meterFacade = MeterFacade.create(meterRegistry, it -> new MeterKey(name, toTags(it)));
            this.keyExtractor = keyExtractor;
            this.tagger = tagger;
        }

        @Override
        public void doOnRequest(long requested) {
            meterFacade.counter(new TypeKey<>(SignalType.REQUEST)).increment();
        }

        @Override
        public void doOnCancel() {
            meterFacade.counter(new TypeKey<>(SignalType.CANCEL)).increment();
        }

        @Override
        public void doOnNext(Alo<T> value) {
            K key = keyExtractor.apply(value.get());
            meterFacade.counter(new TypeKey<>(SignalType.ON_NEXT, key)).increment();
        }

        @Override
        public void doOnError(Throwable error) {
            meterFacade.counter(new TypeKey<>(SignalType.ON_ERROR)).increment();
        }

        private Tags toTags(TypeKey<SignalType, K> typeKey) {
            Collection<Tag> tags = new ArrayList<>();
            tags.add(Tag.of("signal_type", typeKey.typeName()));
            tagger.base().forEach(tags::add);
            typeKey.mapKey(tagger::extract).ifPresent(extractedTags -> extractedTags.forEach(tags::add));
            return Tags.of(tags);
        }
    }
}
