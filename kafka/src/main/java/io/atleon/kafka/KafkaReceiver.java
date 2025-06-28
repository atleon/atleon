package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * A reactive receiver of Kafka {@link ConsumerRecord records}, which are wrapped as
 * {@link KafkaReceiverRecord}. A new Kafka {@link Consumer} is associated with each receiver
 * subscription, and closed if/when that subscription is canceled or errored out. Each reception
 * subscription takes care of keeping track of the records that have been emitted and acknowledged
 * on a per-partition, per-assignment basis, and will only make any given record's offset available
 * for commit (which is done with a configurable periodic interval) if/when that record and all the
 * records that came before it in the same active partition assignment have been acknowledged. It
 * is therefore important that every emitted record be either positively acknowledged when its
 * processing completes normally, or negatively acknowledged ("nacknowledged"), in the case of
 * processing failure.
 * <p>
 * Upon "deactivation" of any assigned partition, whether through typical revocation from a
 * rebalance, reception error, or downstream cancellation, a strong effort is made to ensure that
 * any offsets which can be committed are done so synchronously. The only case in which this is not
 * true is if partitions are signaled to have been "lost", which can happen if/when a consumer is
 * unexpectedly removed from its group (i.e. due to session timeout). For revocation due to typical
 * rebalance, a grace period is configurable, which sets a maximum amount of time that will be
 * awaited for in-flight records to be acknowledged, before attempting final commitment and
 * releasing any assigned partition(s). This grace period can be set to zero such that only record
 * offsets acknowledged at the immediate time of partition deactivation will be committed
 * immediately, which makes rebalancing faster at the cost of higher reprocessing likelihood.
 * <p>
 * This receiver plays well with cooperative rebalancing by allowing polling and processing to
 * continue during a cooperative rebalance, and taking care of cleanup if/when such rebalancing
 * results in partition revocation.
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

    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Collection<String> topics) {
        return receiveManual((consumer, rebalanceListener) -> consumer.subscribe(topics, rebalanceListener));
    }

    public Flux<KafkaReceiverRecord<K, V>> receiveManual(Pattern topicsPattern) {
        return receiveManual((consumer, rebalanceListener) -> consumer.subscribe(topicsPattern));
    }

    public Flux<KafkaReceiverRecord<K, V>> receiveManualWithAssignment(Collection<TopicPartition> topicPartitions) {
        return receiveManual((consumer, rebalanceListener) -> {
            consumer.assign(topicPartitions);
            rebalanceListener.onPartitionsAssigned(topicPartitions);
        });
    }

    @SuppressWarnings("ReactiveStreamsPublisherImplementation")
    private Flux<KafkaReceiverRecord<K, V>> receiveManual(AssignmentSpec assignmentSpec) {
        return Flux.from(it -> it.onSubscribe(subscriptionFactory.periodic(assignmentSpec, it)));
    }
}
