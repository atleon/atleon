package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * A reactive receiver of Kafka {@link ConsumerRecord records}, which may be wrapped as
 * {@link KafkaReceiverRecord}. A new Kafka {@link Consumer} is associated with each receiver
 * subscription, and closed if/when that subscription is canceled or errored out. Each reception
 * subscription takes care of keeping track of the records that have been emitted and acknowledged
 * on a per-partition, per-assignment basis, and will only make any given record's offset available
 * for commitment (triggered by periodic or batch sizing) if/when that record and all the records
 * that came before it in the same actively assigned partition have been acknowledged. It is
 * therefore important that every emitted record be either positively acknowledged when its
 * processing completes normally, or negatively acknowledged ("nacknowledged"), in the case of
 * processing failure.
 * <p>
 * <b>Reception Modes: </b> This receiver provides the following modes of reception:
 * <ul>
 * <li><b>Periodic Commit:</b> The default reception mode uses a periodic commit strategy, where
 * acknowledged offsets are committed either after acknowledging a configurable number of records
 * or after a configurable amount of time has passed after acknowledging an uncommitted offset.
 * Upon "deactivation" of any assigned partition, whether through typical revocation from a
 * rebalance, reception error, or downstream cancellation, a strong effort is made to ensure that
 * any offsets which can be committed are done so synchronously. The only case in which this is not
 * true is if partitions are signaled to have been "lost", which can happen if/when a consumer is
 * unexpectedly removed from its group (i.e. due to session timeout). For revocation due to typical
 * rebalance, a grace period is configurable, which sets a maximum amount of time that will be
 * awaited for in-flight records to be acknowledged before attempting final commitment and
 * releasing any assigned partition(s). This grace period can be set to zero such that only record
 * offsets acknowledged at the immediate time of partition deactivation will be committed, which
 * makes rebalancing faster at the cost of higher reprocessing likelihood. Periodic commit
 * reception mode plays well with cooperative rebalancing by allowing polling and processing to
 * continue during a cooperative rebalance, and taking care of cleanup if/when such rebalancing
 * results in partition revocation.</li>
 * <li><b>Transactional:</b> When configured for transactional polling (using a receiveTx method
 * and {@link KafkaTxManager}), each emission cycle is managed within a transaction. Records are
 * polled and emitted within transaction sessions, and their offsets are sent as part of the
 * transaction before committing. The transaction is only committed when all records in the session
 * have been acknowledged. If reception is canceled, an error occurs, or if a partition is
 * lost, any open transaction is aborted. This allows for exactly-once processing semantics for
 * records and their offsets. The size of each transaction session (in terms of record count) and
 * commit interval are configurable, and transactional state is managed per subscription.
 * Transactional polling guarantees that offsets are only committed if all records emitted in the
 * transaction have been successfully processed and acknowledged.</li>
 * </ul>
 * <p>
 * <b>Emission Acknowledgement Modes:</b> There are two modes of acknowledgement for emitted
 * records:
 * <ul>
 * <li><b>Manual Acknowledgement:</b> This is the default acknowledgement mode. Every emitted
 * record <i>must</i> be acknowledged either positively or negatively after emission.</li>
 * <li><b>Auto Acknowledgement:</b> This emission mode provides simplified processing by
 * automatically acknowledging records upon successful completion of downstream <i>emission</i>.
 * Each {@link ConsumerRecord} is wrapped in a {@link Mono} that automatically triggers
 * acknowledgement when the Mono completes normally (i.e., {@link SignalType#ON_COMPLETE}). This
 * eliminates the need for explicit acknowledgement calls in application code, making it suitable
 * for fire-and-forget processing patterns. However, this mode has important limitations: it does
 * <i>not</i> guarantee at-least-once processing semantics in the presence of downstream
 * asynchronous boundaries (such as {@code flatMap}, {@code publishOn}, or other operators that
 * introduce asynchrony), since acknowledgement occurs based on Mono completion rather than actual
 * processing completion. For reliable processing with async operations, use manual
 * acknowledgement.</li>
 * </ul>
 * <p>
 * <b>Acknowledgement Queueing Modes:</b> This receiver maintains acknowledgement queues to ensure
 * proper offset commitment ordering within each topic-partition. Two modes control how
 * acknowledged offsets become eligible for commitment (both maintain identical at-least-once
 * delivery semantics and ensure that no offset is made eligible for commitment until all preceding
 * records in the actively assigned partition have been acknowledged):
 * <ul>
 * <li><b>STRICT Mode:</b> The default mode where each acknowledged record's offset becomes
 * eligible for commitment individually.</li>
 * <li><b>COMPACT Mode:</b> An optimization where acknowledgement tracking may be consolidated when
 * multiple sequential records are acknowledged but not yet eligible for commitment, reducing the
 * granularity of offset eligibility tracking. This mode is particularly beneficial for
 * high-throughput, high-concurrency scenarios (such as work queue processing) where
 * acknowledgements are likely to occur out of order. Note that this mode removes the strong
 * coupling of record processing to commit batch sizing (since this is based on the number of
 * offsets made eligible for commitment), so it is rather recommended to depend on time-based
 * commit triggering when using this mode.</li>
 * </ul>
 * <p>
 * <b>Polling Strategies:</b> This receiver supports configurable polling strategies to optimize
 * partition selection during consumer poll invocations. The polling strategy determines which
 * assigned partitions are polled in each polling cycle, enabling fine-tuned control over partition
 * consumption. There are a few built-in polling strategies available:
 * <ul>
 * <li><b>Natural (Default):</b> Polls from all assigned (and non-externally paused) partitions
 * using default Kafka consumer behavior. This strategy makes no guarantees about consumption bias
 * or fairness</li>
 * <li><b>Binary Strides:</b> A balanced strategy that polls approximately half of assigned
 * partitions per cycle using binary striding selection. This creates more even consumption across
 * partitions and can reduce the likelihood or performance impacts caused by one or two highly
 * lagging assigned partitions.</li>
 * <li><b>Greatest Batch Lag:</b> Prioritizes partitions with the highest lag in units of the
 * polling batch size. This strategy is useful when prioritizing uniform lag across all assigned
 * partitions</li>
 * </ul>
 * <p>
 * Polling strategies can be configured via 
 * {@link KafkaReceiverOptions.Builder#pollStrategyFactory(PollStrategyFactory)}. Custom strategies
 * can be implemented by providing a {@link PollStrategyFactory} that creates instances of
 * {@link PollStrategy}.
 *
 * @param <K> The type of keys in records emitted by this receiver
 * @param <V> The type of values in records emitted by this receiver
 */
public final class KafkaReceiver<K, V> {

    private final PollingSubscriptionFactory<K, V> subscriptionFactory;

    private KafkaReceiver(KafkaReceiverOptions<K, V> options) {
        this.subscriptionFactory = new PollingSubscriptionFactory<>(options);
    }

    public static <K, V> KafkaReceiver<K, V> create(KafkaReceiverOptions<K, V> options) {
        return new KafkaReceiver<>(options);
    }

    /**
     * Receive records from the specified topics with automatic acknowledgment.
     * <p>
     * Each {@link ConsumerRecord} is wrapped as a {@link Mono}, which, upon successful emission
     * completion (i.e., {@link reactor.core.publisher.SignalType#ON_COMPLETE}), will result in
     * automatically acknowledging the originating {@link KafkaReceiverRecord}. Note that
     * downstream errors typically result in cancellation.
     * <p>
     * <b>Important</b>: This mode of reception does <i>not</i> guarantee processing (at least
     * once) in the presence of downstream asynchronous boundaries.
     *
     * @param topics The collection of topic names to subscribe to
     * @return A {@link Flux} of {@link Mono} instances, each wrapping a {@link ConsumerRecord}
     * with automatic acknowledgment behavior
     */
    public Flux<Mono<ConsumerRecord<K, V>>> receiveAutoAck(Collection<String> topics) {
        return receiveAutoAck(newAssignmentSpec(topics));
    }

    /**
     * Receive records from topics matching the given pattern with automatic acknowledgment.
     * <p>
     * Each {@link ConsumerRecord} is wrapped as a {@link Mono}, which, upon successful emission
     * completion (i.e., {@link reactor.core.publisher.SignalType#ON_COMPLETE}), will result in
     * automatically acknowledging the originating {@link KafkaReceiverRecord}. Note that
     * downstream errors typically result in cancellation.
     * <p>
     * <b>Important</b>: This mode of reception does <i>not</i> guarantee processing (at least
     * once) in the presence of downstream asynchronous boundaries.
     *
     * @param topicPattern The pattern to match topic names for subscription
     * @return A {@link Flux} of {@link Mono} instances, each wrapping a {@link ConsumerRecord}
     * with automatic acknowledgment behavior
     */
    public Flux<Mono<ConsumerRecord<K, V>>> receiveAutoAck(Pattern topicPattern) {
        return receiveAutoAck(newAssignmentSpec(topicPattern));
    }

    /**
     * Receive records from the specified partitions with automatic acknowledgment.
     * <p>
     * Each {@link ConsumerRecord} is wrapped as a {@link Mono}, which, upon successful emission
     * completion (i.e., {@link reactor.core.publisher.SignalType#ON_COMPLETE}), will result in
     * automatically acknowledging the originating {@link KafkaReceiverRecord}. Note that
     * downstream errors typically result in cancellation.
     * <p>
     * <b>Important</b>: This mode of reception does <i>not</i> guarantee processing (at least
     * once) in the presence of downstream asynchronous boundaries.
     *
     * @param topicPartitions The collection of specific topic partitions to assign
     * @return A {@link Flux} of {@link Mono} instances, each wrapping a {@link ConsumerRecord}
     * with automatic acknowledgment behavior
     */
    public Flux<Mono<ConsumerRecord<K, V>>> receiveAutoAckWithAssignment(Collection<TopicPartition> topicPartitions) {
        return receiveAutoAck(newManualAssignmentSpec(topicPartitions));
    }

    /**
     * Receive records from the given topics with manual acknowledgment control.
     *
     * @param topics The topics to subscribe to
     * @return A stream of records that require manual acknowledgment
     */
    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Collection<String> topics) {
        return receiveManual(newAssignmentSpec(topics));
    }

    /**
     * Receive records from topics matching the given pattern with manual acknowledgment control.
     *
     * @param topicsPattern The pattern to match topics to subscribe to
     * @return A stream of records that require manual acknowledgment
     */
    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Pattern topicsPattern) {
        return receiveManual(newAssignmentSpec(topicsPattern));
    }

    /**
     * Receive records from the given topics with manual acknowledgment, using a provided
     * transaction manager to ensure records are processed and committed with a transaction.
     *
     * @param txManager The manager to use for transactional processing
     * @param topics    The topics to subscribe to
     * @return A stream of records that require manual acknowledgment
     */
    public Flux<KafkaReceiverRecord<K, V>> receiveTxManual(
        Publisher<? extends KafkaTxManager> txManager,
        Collection<String> topics
    ) {
        return Mono.from(txManager).flatMapMany(it -> receiveTxManual(it, newAssignmentSpec(topics)));
    }

    /**
     * Receive records from the given topics with manual acknowledgment, using a provided
     * transaction manager to ensure records are processed and committed with a transaction.
     *
     * @param txManager     The manager to use for transactional processing
     * @param topicsPattern The pattern to match topics to subscribe to
     * @return A stream of records that require manual acknowledgment
     */
    public Flux<KafkaReceiverRecord<K, V>> receiveTxManual(
        Publisher<? extends KafkaTxManager> txManager,
        Pattern topicsPattern
    ) {
        return Mono.from(txManager).flatMapMany(it -> receiveTxManual(it, newAssignmentSpec(topicsPattern)));
    }

    private Flux<Mono<ConsumerRecord<K, V>>> receiveAutoAck(AssignmentSpec assignmentSpec) {
        return receiveManual(assignmentSpec).map(receiverRecord -> {
            Runnable acknowledger = receiverRecord.acknowledger();
            return Mono.just(receiverRecord.consumerRecord()).doFinally(signalType -> {
                if (signalType == SignalType.ON_COMPLETE) {
                    acknowledger.run();
                }
            });
        });
    }

    @SuppressWarnings("ReactiveStreamsPublisherImplementation")
    private Flux<KafkaReceiverRecord<K, V>> receiveManual(AssignmentSpec assignmentSpec) {
        return Flux.from(it -> it.onSubscribe(subscriptionFactory.periodicCommit(assignmentSpec, it)));
    }

    @SuppressWarnings("ReactiveStreamsPublisherImplementation")
    private Flux<KafkaReceiverRecord<K, V>> receiveTxManual(KafkaTxManager txManager, AssignmentSpec assignmentSpec) {
        return Flux.from(it -> it.onSubscribe(subscriptionFactory.transactional(txManager, assignmentSpec, it)));
    }

    private static AssignmentSpec newAssignmentSpec(Collection<String> topics) {
        return (consumer, rebalanceListener) -> consumer.subscribe(topics, rebalanceListener);
    }

    private static AssignmentSpec newAssignmentSpec(Pattern topicsPattern) {
        return (consumer, rebalanceListener) -> consumer.subscribe(topicsPattern, rebalanceListener);
    }

    private static AssignmentSpec newManualAssignmentSpec(Collection<TopicPartition> topicPartitions) {
        return (consumer, rebalanceListener) -> {
            consumer.assign(topicPartitions);
            rebalanceListener.onPartitionsAssigned(topicPartitions);
        };
    }
}
