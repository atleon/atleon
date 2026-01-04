package io.atleon.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

class AsyncOffsetCommitterTest {

    @Test
    public void commit_givenNonRetriableFailure_expectsError() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        Consumer<?, ?> consumer = mock(Consumer.class);
        AtomicReference<Throwable> error = new AtomicReference<>();

        AsyncOffsetCommitter committer =
                new AsyncOffsetCommitter(Integer.MAX_VALUE, it -> it.accept(consumer), error::set);
        committer.schedulePeriodically(1, Duration.ofSeconds(1), Schedulers.parallel());

        doAnswer(invocation -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
                    OffsetCommitCallback callback = invocation.getArgument(1);
                    callback.onComplete(offsets, new UnsupportedOperationException("Boom"));
                    return null;
                })
                .when(consumer)
                .commitAsync(anyMap(), any(OffsetCommitCallback.class));

        committer
                .acknowledgementHandlerForAssigned(topicPartition)
                .accept(new AcknowledgedOffset(topicPartition, new OffsetAndMetadata(1L)));

        assertInstanceOf(UnsupportedOperationException.class, error.get());
        assertFalse(committer.isCommitTrialExhausted(topicPartition));
    }

    @Test
    public void commit_givenRetriableFailureWithEventualSuccess_expectsRetryAndSuccess() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        Consumer<?, ?> consumer = mock(Consumer.class);
        AtomicReference<Throwable> error = new AtomicReference<>();

        AsyncOffsetCommitter committer =
                new AsyncOffsetCommitter(Integer.MAX_VALUE, it -> it.accept(consumer), error::set);
        committer.schedulePeriodically(1, Duration.ofSeconds(1), Schedulers.parallel());

        AtomicInteger commitAttempts = new AtomicInteger(0);
        doAnswer(invocation -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
                    OffsetCommitCallback callback = invocation.getArgument(1);
                    int attempt = commitAttempts.incrementAndGet();
                    if (attempt == 1) {
                        // First attempt fails with retriable error
                        callback.onComplete(offsets, new RetriableCommitFailedException("Persistent error"));
                    } else {
                        // Subsequent attempts succeed
                        callback.onComplete(offsets, null);
                    }
                    return null;
                })
                .when(consumer)
                .commitAsync(anyMap(), any(OffsetCommitCallback.class));

        committer
                .acknowledgementHandlerForAssigned(topicPartition)
                .accept(new AcknowledgedOffset(topicPartition, new OffsetAndMetadata(1L)));

        assertEquals(2, commitAttempts.get());
        assertNull(error.get());
        assertFalse(committer.isCommitTrialExhausted(topicPartition));
    }

    @Test
    public void commit_givenRetriableFailureWithRetriesExhausted_expectsError() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        int maxCommitAttempts = 3;
        Consumer<?, ?> consumer = mock(Consumer.class);
        AtomicReference<Throwable> error = new AtomicReference<>();

        AsyncOffsetCommitter committer =
                new AsyncOffsetCommitter(maxCommitAttempts, it -> it.accept(consumer), error::set);
        committer.schedulePeriodically(1, Duration.ofSeconds(1), Schedulers.parallel());

        AtomicInteger commitAttempts = new AtomicInteger(0);
        doAnswer(invocation -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
                    OffsetCommitCallback callback = invocation.getArgument(1);
                    commitAttempts.incrementAndGet();
                    // Always fail with retriable error
                    callback.onComplete(offsets, new RetriableCommitFailedException("Persistent error"));
                    return null;
                })
                .when(consumer)
                .commitAsync(anyMap(), any(OffsetCommitCallback.class));

        committer
                .acknowledgementHandlerForAssigned(topicPartition)
                .accept(new AcknowledgedOffset(topicPartition, new OffsetAndMetadata(1L)));

        assertEquals(maxCommitAttempts, commitAttempts.get());
        assertInstanceOf(KafkaException.class, error.get());
        assertInstanceOf(RetriableCommitFailedException.class, error.get().getCause());
        assertTrue(committer.isCommitTrialExhausted(topicPartition));
    }
}
