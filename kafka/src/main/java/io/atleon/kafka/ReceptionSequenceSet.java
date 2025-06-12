package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sequence counter(s) for operations on partitions that have been assigned at some point during
 * reception.
 */
final class ReceptionSequenceSet {

    // Monotonically increasing assignment sequence numbers. Newer sequence numbers invalidate
    // committable offsets with older sequence numbers.
    private final Map<TopicPartition, AtomicLong> assignments = new ConcurrentHashMap<>();

    // Monotonically increasing commit sequence numbers. Newer sequence numbers invalidate offsets
    // whose commitment is in-flight with older sequence numbers.
    private final Map<TopicPartition, AtomicLong> commits = new ConcurrentHashMap<>();

    // Consecutive commit retry sequence numbers.
    private final Map<TopicPartition, AtomicInteger> commitRetries = new ConcurrentHashMap<>();

    public long assigned(TopicPartition partition) {
        AtomicLong assignment = assignments.computeIfAbsent(partition, __ -> new AtomicLong(0));
        commits.computeIfAbsent(partition, __ -> new AtomicLong(0));
        commitRetries.computeIfAbsent(partition, __ -> new AtomicInteger(0));
        return assignment.incrementAndGet();
    }

    public void unassigned(TopicPartition partition) {
        // For a partition that is no longer assigned, increment its assignment sequence such that
        // emitted offsets for which commitment has not yet been attempted are invalidated,
        // increment its commit sequence such that any offsets for which commitment is currently
        // being attempted/retried are invalidated, and reset its commit retry count. Keep the
        // entries for this partition around, though, in case it is quickly reassigned, which could
        // theoretically happen while there is still an in-flight commit from previous assignment.
        // Note that this method is always invoked from the polling thread, and the Kafka client
        // takes care of invoking this after clearing out any in-flight asynchronous commit
        // invocations, so none should be concurrently executing at this point. Also note that this
        // method is only called when it is known that the provided partition was previously
        // assigned (so no null-check is necessary).
        assignments.get(partition).incrementAndGet();
        incrementAndGetCommit(partition);
        resetCommitRetry(partition);
    }

    public long getAssignment(TopicPartition partition) {
        return assignments.get(partition).get();
    }

    public long incrementAndGetCommit(TopicPartition partition) {
        return commits.get(partition).incrementAndGet();
    }

    public long getCommit(TopicPartition partition) {
        return commits.get(partition).get();
    }

    public int getCommitRetry(TopicPartition partition) {
        return commitRetries.get(partition).get();
    }

    public void incrementCommitRetry(TopicPartition partition) {
        commitRetries.get(partition).incrementAndGet();
    }

    public void resetCommitRetry(TopicPartition partition) {
        commitRetries.get(partition).set(0);
    }
}
