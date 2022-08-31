package io.atleon.spring;

import io.atleon.core.AloStream;

public interface ConfiguredAloStream {
    void start();

    void stop();

    String name();

    AloStream.State state();
}
