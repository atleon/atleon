package io.atleon.rabbitmq;

import io.atleon.core.Alo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

public class DefaultAloRabbitMQSenderResultSubscriber<T> extends BaseSubscriber<Alo<RabbitMQSenderResult<T>>> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultAloRabbitMQSenderResultSubscriber.class);

    @Override
    protected void hookOnNext(Alo<RabbitMQSenderResult<T>> aloResult) {
        RabbitMQSenderResult<T> result = aloResult.get();
        if (result.isAck()) {
            Alo.acknowledge(aloResult);
        } else {
            LOGGER.warn("RabbitMQ Sender Result is not an ack: result={}", result);
            Alo.nacknowledge(aloResult, new UnackedRabbitMQSenderResultException());
        }
    }

    private static final class UnackedRabbitMQSenderResultException extends Exception {

    }
}
