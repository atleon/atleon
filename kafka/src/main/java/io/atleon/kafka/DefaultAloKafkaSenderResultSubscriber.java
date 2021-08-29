package io.atleon.kafka;

import io.atleon.core.Alo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

import java.util.Optional;

public class DefaultAloKafkaSenderResultSubscriber<T> extends BaseSubscriber<Alo<KafkaSenderResult<T>>> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultAloKafkaSenderResultSubscriber.class);

    @Override
    protected void hookOnNext(Alo<KafkaSenderResult<T>> aloResult) {
        KafkaSenderResult<T> result = aloResult.get();
        Optional<Exception> error = result.exception();
        if (error.isPresent()) {
            LOGGER.warn("Alo Kafka Sender Result has Error: result={}", result);
            Alo.nacknowledge(aloResult, error.get());
        } else {
            Alo.acknowledge(aloResult);
        }
    }
}
