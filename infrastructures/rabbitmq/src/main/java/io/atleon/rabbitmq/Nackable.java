package io.atleon.rabbitmq;

public interface Nackable {

    void nack(boolean requeue);
}
