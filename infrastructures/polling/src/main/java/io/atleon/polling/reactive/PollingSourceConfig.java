package io.atleon.polling.reactive;

import io.atleon.polling.AloPollingReceiver;
import java.time.Duration;
import java.util.Optional;

public class PollingSourceConfig {

    private final Duration pollingInterval;
    private final AloPollingReceiver.NackStrategy nackStrategy;

    public PollingSourceConfig(final Duration pollingInterval) {
        this(pollingInterval, null);
    }

    public PollingSourceConfig(final Duration pollingInterval, final AloPollingReceiver.NackStrategy nackStrategy) {
        this.pollingInterval = pollingInterval;
        this.nackStrategy = nackStrategy;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public AloPollingReceiver.NackStrategy getNackStrategy() {
        return Optional.ofNullable(nackStrategy).orElse(AloPollingReceiver.NackStrategy.NACK_EMIT);
    }
}
