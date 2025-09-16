package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PollerImpTest extends AbstractPollingTest{

    private Poller<String, String> poller;
    private List<String> events;

    @BeforeEach
    public void init() {
        events = Arrays.asList("Test1", "Test2", "Test3");
        Pollable<String, String> pollable = new TestPollable(events);
        poller = Poller.create(pollable, Duration.ofSeconds(2));
    }

    @Test
    public void testReceive() {
         poller.receive()
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .expectNextMatches(strings -> strings.stream()
                         .map(Polled::getPayload).collect(Collectors.toList())
                         .containsAll(events))
                 .thenCancel()
                 .verify();
    }

    @Test
    public void testClose() {
        poller.receive()
                .as(StepVerifier::create)
                .expectNextCount(1)
                .expectNextMatches(strings -> strings.stream()
                                .map(Polled::getPayload)
                                .collect(Collectors.toList()).
                        containsAll(events))
                .then(poller::close)
                .expectNextCount(0)
                .thenCancel()
                .verify();
    }
}
