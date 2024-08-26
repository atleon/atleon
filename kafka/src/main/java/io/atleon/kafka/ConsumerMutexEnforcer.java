package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Enforces condition that at most one {@link Consumer} sourced from this class may be active at
 * any given instant, and allows prohibiting further consumption after some specified grace period.
 * This helps to work around an observed bug in Reactor Kafka where, upon re-subscription, it is
 * possible for a Consumer and its event loop to become orphaned, yet continue to poll with paused
 * partitions, and therefore silently occupy assigned partitions without doing any processing.
 */
class ConsumerMutexEnforcer extends ConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerMutexEnforcer.class);

    private final AtomicInteger ticketCounter = new AtomicInteger(0);

    public final ProhibitableConsumerFactory newConsumerFactory() {
        return new ProhibitableConsumerFactory(ticketCounter.incrementAndGet());
    }

    protected <K, V> Consumer<K, V> newConsumer(ReceiverOptions<K, V> options) {
        return new KafkaConsumer<>(options.consumerProperties(), options.keyDeserializer(), options.valueDeserializer());
    }

    final class ProhibitableConsumerFactory extends ConsumerFactory {

        private final int ticket;

        private volatile Instant consumptionDeadline = null;

        private ProhibitableConsumerFactory(int ticket) {
            this.ticket = ticket;
        }

        @Override
        public <K, V> Consumer<K, V> createConsumer(ReceiverOptions<K, V> config) {
            return new ConsumerProxy<>(config);
        }

        public void prohibitFurtherConsumption(Duration gracePeriod) {
            consumptionDeadline = Instant.now().plus(gracePeriod);
        }

        private final class ConsumerProxy<K, V> implements Consumer<K, V> {

            private final String clientId;

            private final Consumer<K, V> delegate;

            public ConsumerProxy(ReceiverOptions<K, V> options) {
                this.clientId = options.clientId();
                this.delegate = newConsumer(options);
            }

            @Override
            public Set<TopicPartition> assignment() {
                return delegate.assignment();
            }

            @Override
            public Set<String> subscription() {
                return delegate.subscription();
            }

            @Override
            public void subscribe(Collection<String> topics) {
                delegate.subscribe(topics);
            }

            @Override
            public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
                delegate.subscribe(topics, callback);
            }

            @Override
            public void assign(Collection<TopicPartition> partitions) {
                delegate.assign(partitions);
            }

            @Override
            public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
                delegate.subscribe(pattern, callback);
            }

            @Override
            public void subscribe(Pattern pattern) {
                delegate.subscribe(pattern);
            }

            @Override
            public void unsubscribe() {
                delegate.unsubscribe();
            }

            @Override
            @Deprecated
            public ConsumerRecords<K, V> poll(long timeout) {
                return poll(Duration.ofMillis(timeout));
            }

            @Override
            public ConsumerRecords<K, V> poll(Duration timeout) {
                return invokeIfAllowed(() -> delegate.poll(timeout));
            }

            @Override
            public void commitSync() {
                delegate.commitSync();
            }

            @Override
            public void commitSync(Duration timeout) {
                delegate.commitSync(timeout);
            }

            @Override
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                delegate.commitSync(offsets);
            }

            @Override
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
                delegate.commitSync(offsets, timeout);
            }

            @Override
            public void commitAsync() {
                delegate.commitAsync();
            }

            @Override
            public void commitAsync(OffsetCommitCallback callback) {
                delegate.commitAsync(callback);
            }

            @Override
            public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
                delegate.commitAsync(offsets, callback);
            }

            @Override
            public void seek(TopicPartition partition, long offset) {
                delegate.seek(partition, offset);
            }

            @Override
            public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
                delegate.seek(partition, offsetAndMetadata);
            }

            @Override
            public void seekToBeginning(Collection<TopicPartition> partitions) {
                delegate.seekToBeginning(partitions);
            }

            @Override
            public void seekToEnd(Collection<TopicPartition> partitions) {
                delegate.seekToEnd(partitions);
            }

            @Override
            public long position(TopicPartition partition) {
                return delegate.position(partition);
            }

            @Override
            public long position(TopicPartition partition, Duration timeout) {
                return delegate.position(partition, timeout);
            }

            @Override
            @Deprecated
            public OffsetAndMetadata committed(TopicPartition partition) {
                return delegate.committed(partition);
            }

            @Override
            @Deprecated
            public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
                return delegate.committed(partition, timeout);
            }

            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
                return delegate.committed(partitions);
            }

            @Override
            public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
                return delegate.committed(partitions, timeout);
            }

            @Override
            public Map<MetricName, ? extends Metric> metrics() {
                return delegate.metrics();
            }

            @Override
            public List<PartitionInfo> partitionsFor(String topic) {
                return delegate.partitionsFor(topic);
            }

            @Override
            public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
                return delegate.partitionsFor(topic, timeout);
            }

            @Override
            public Map<String, List<PartitionInfo>> listTopics() {
                return delegate.listTopics();
            }

            @Override
            public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
                return delegate.listTopics(timeout);
            }

            @Override
            public Set<TopicPartition> paused() {
                return delegate.paused();
            }

            @Override
            public void pause(Collection<TopicPartition> partitions) {
                delegate.pause(partitions);
            }

            @Override
            public void resume(Collection<TopicPartition> partitions) {
                delegate.resume(partitions);
            }

            @Override
            public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
                return delegate.offsetsForTimes(timestampsToSearch);
            }

            @Override
            public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
                return delegate.offsetsForTimes(timestampsToSearch, timeout);
            }

            @Override
            public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
                return delegate.beginningOffsets(partitions);
            }

            @Override
            public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
                return delegate.beginningOffsets(partitions, timeout);
            }

            @Override
            public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
                return delegate.endOffsets(partitions);
            }

            @Override
            public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
                return delegate.endOffsets(partitions, timeout);
            }

            @Override
            public OptionalLong currentLag(TopicPartition topicPartition) {
                return delegate.currentLag(topicPartition);
            }

            @Override
            public ConsumerGroupMetadata groupMetadata() {
                return delegate.groupMetadata();
            }

            @Override
            public void enforceRebalance() {
                delegate.enforceRebalance();
            }

            @Override
            public void enforceRebalance(String reason) {
                delegate.enforceRebalance(reason);
            }

            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public void close(Duration timeout) {
                delegate.close(timeout);
            }

            @Override
            public void wakeup() {
                delegate.wakeup();
            }

            private <T> T invokeIfAllowed(Supplier<T> supplier) {
                Instant deadline = consumptionDeadline;
                if (deadline != null && Instant.now().compareTo(deadline) > 0) {
                    RuntimeException exception =
                        new IllegalStateException("Consumer invocations prohibited, but invoked: " + clientId);
                    LOGGER.error(exception.getMessage(), exception);
                    throw exception;
                } else if (deadline == null && ticket != ticketCounter.get()) {
                    RuntimeException exception =
                        new IllegalStateException("Detected orphaned Consumer instance: " + clientId);
                    LOGGER.error(exception.getMessage(), exception);
                    throw exception;
                } else {
                    return supplier.get();
                }
            }
        }
    }
}
