package io.atleon.core;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.util.function.Function;

final class ResubscribingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResubscribingTransformer.class);

    private final ResubscriptionConfig config;

    ResubscribingTransformer(ResubscriptionConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? applyResubscription(publisher) : publisher;
    }

    private Flux<T> applyResubscription(Publisher<T> publisher) {
        return Flux.from(publisher).retryWhen(Retry.from(this::scheduleResubscription));
    }

    private Flux<?> scheduleResubscription(Flux<Retry.RetrySignal> signals) {
        return signals
            .doOnNext(signal ->
                LOGGER.error("An Error has occurred! Scheduling resubscription: name={} errorNo={} delay={}",
                    config.getName(), signal.totalRetries() + 1, config.getDelay(), signal.failure()))
            .delayElements(config.getDelay())
            .doOnNext(signal ->
                LOGGER.info("Attempting resubscription from Error: name={} errorNo={}",
                    config.getName(), signal.totalRetries() + 1));
    }
}
