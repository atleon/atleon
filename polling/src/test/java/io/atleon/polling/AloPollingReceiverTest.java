package io.atleon.polling;

import io.atleon.polling.reactive.PollingSourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AloPollingReceiverTest {

    private Pollable<TestMessage, TestMessage> pollable;
    private PollingSourceConfig config;

    @BeforeEach
    public void init() {
        config = new PollingSourceConfig(Duration.ofSeconds(3));
    }

    @Test
    public void testTenMessagesAllSuccessful() throws Exception {
        final Queue<Set<TestMessage>> messages = new LinkedList<>();
        final Set<TestMessage> acks = new HashSet<>();
        final Set<TestMessage> nacks = new HashSet<>();
        messages.add(buildSuccessMessages("Batch - 1",5));
        messages.add(buildSuccessMessages("Batch - 2", 2));
        messages.add(buildSuccessMessages("Batch - 3", 3));
        pollable = new TestPollable(messages, acks, nacks);
        AloPollingReceiver<TestMessage, TestMessage> receiver = AloPollingReceiver.from(pollable, config);
        receiver.receivePayloads()
                .resubscribeOnError(AloPollingReceiverTest.class.getSimpleName(), Duration.ofMillis(1000))
                .subscribe();

        while (acks.size() < 10) {
            Thread.sleep(1000L);
        }
        assertEquals(0, nacks.size());
        assertEquals(10, acks.size());
    }

    @Test
    public void testTenMessagesHalfSuccessfulHalfFail() throws Exception {
        final Queue<Set<TestMessage>> messages = new LinkedList<>();
        final Set<TestMessage> acks = new HashSet<>();
        final Set<TestMessage> nacks = new HashSet<>();
        messages.add(buildSuccessMessages("Batch - 1", 5));
        messages.add(buildFailureMessages("Batch - 2",5, 1));
        pollable = new TestPollable(messages, acks, nacks);
        AloPollingReceiver<TestMessage, TestMessage> receiver = AloPollingReceiver.from(pollable, config);
        receiver.receivePayloads()
                .map(TestMessage::getMessage)
                .resubscribeOnError(AloPollingReceiverTest.class.getSimpleName(), Duration.ofSeconds(1))
                .subscribe();

        while (acks.size() < 10) {
            Thread.sleep(1000L);
        }
        assertEquals(10, acks.size());
        assertEquals(5, nacks.size());
    }

    private Set<TestMessage> buildSuccessMessages(final String message,
                                                  final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new TestSuccessMessage(i, message + " Test Message " + i))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Set<TestMessage> buildFailureMessages(final String message,
                                                  int count,
                                                  int exceptionCount) {
        return IntStream.range(0, count)
                .mapToObj(i -> new TestFailureMessage(i, message + " Test Message " + i, exceptionCount))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static class TestPollable implements Pollable<TestMessage, TestMessage> {

        private final Queue<Set<TestMessage>> messages;
        private final Set<TestMessage> acks;
        private final Set<TestMessage> nacks;

        public TestPollable(final Queue<Set<TestMessage>> messages,
                            final Set<TestMessage> acks,
                            final Set<TestMessage> nacks) {
            this.messages = messages;
            this.acks = acks;
            this.nacks = nacks;
        }

        @Override
        public Collection<Polled<TestMessage, TestMessage>> poll() {
            Optional.ofNullable(messages.peek())
                    .ifPresent(m -> {
                        if (m.isEmpty()) {
                            messages.poll();
                        }
                    });
            return Optional.ofNullable(messages.peek())
                    .map(msgs -> msgs.stream()
                            .map(m -> Polled.compose(m, m))
                            .collect(Collectors.toList()))
                    .orElse(new ArrayList<>());
        }

        @Override
        public synchronized void ack(TestMessage event) {
            Optional.ofNullable(messages.peek()).orElse(new LinkedHashSet<>())
                            .remove(event);
            acks.add(event);
        }

        @Override
        public synchronized void nack(Throwable t, TestMessage event) {
            nacks.add(event);
        }
    }

    private interface TestMessage {
        int getId();
        String getMessage();
    }

    private static class TestSuccessMessage implements TestMessage {
        private final int id;
        private final String message;

        TestSuccessMessage(final int id,
                    final String message) {
            this.id = id;
            this.message = message;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public String getMessage() {
            return message;
        }

    }

    private static class TestFailureMessage implements TestMessage {

        private final int id;
        private final String message;
        private final  int exceptionCount;
        private final AtomicInteger count = new AtomicInteger(0);

        TestFailureMessage(final int id,
                           final String message,
                           int exceptionCount) {
            this.id = id;
            this.message = message;
            this.exceptionCount = exceptionCount;
        }


        @Override
        public int getId() {
            return id;
        }

        @Override
        public String getMessage() {
            if (count.getAndIncrement() < exceptionCount) {
                throw new RuntimeException("Boom");
            }
            return message;
        }
    }
}
