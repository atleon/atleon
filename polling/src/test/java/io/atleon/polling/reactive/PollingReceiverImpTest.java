package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class PollingReceiverImpTest extends AbstractPollingTest {

    private PollingReceiver<String, String> pollingReceiver;

    @BeforeEach
    public void init() {
        List<String> events = Arrays.asList("Test", "Test2", "Test3");
        Pollable<String, String> pollable = new TestPollable(events);
        pollingReceiver = PollingReceiver.create(pollable,
                PollerOptions.create(Duration.ofSeconds(2), () -> Schedulers.newSingle("Testing")));
    }

    @Test
    public void testReceive() {
        pollingReceiver.receive()
                .as(StepVerifier::create)
                .expectNextMatches(r -> r.getRecord().getPayload().equals("Test"))
                .expectNextMatches(r -> r.getRecord().getPayload().equals("Test2"))
                .expectNextMatches(r -> r.getRecord().getPayload().equals("Test3"))
                .thenCancel()
                .verify();
    }
}
