package io.atleon.rabbitmq;

import java.util.Map;

public interface Configurable {

    void configure(Map<String, ?> properties);
}
