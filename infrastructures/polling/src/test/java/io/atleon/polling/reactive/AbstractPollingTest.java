package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;

import java.util.Collection;
import java.util.stream.Collectors;

public abstract class AbstractPollingTest {

    protected static final class TestPollable implements Pollable<String, String> {

        private final Collection<String> events;

        TestPollable(final Collection<String> events) {
            this.events = events;
        }

        @Override
        public Collection<Polled<String, String>> poll() {
            return events.stream().map(e -> Polled.compose(e, e)).collect(Collectors.toList());
        }

        @Override
        public void ack(String event) {}

        @Override
        public void nack(Throwable t, String event) {}
    }
}
