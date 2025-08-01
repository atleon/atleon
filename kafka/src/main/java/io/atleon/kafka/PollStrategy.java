package io.atleon.kafka;

import io.atleon.util.Collecting;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * Interface through which selection of Kafka partitions from which to poll are selected.
 * Implementations of this interface are kept apprised of "permitted" and (subsequently)
 * "prohibited" partitions available for polling. A request for "preparation" is performed before
 * the invocation of every un-paused (i.e. not paused due to back-pressure) polling cycle.
 * "Preparation" should invoke some selection of "permitted" partitions on the provided
 * {@link PollSelectionContext}. A "permitted" partition is one that is currently assigned and not
 * forcefully/externally paused, and a "prohibited" partition is any partition that does not meet
 * that criteria. Note that the methods invoked on this interface are always done so via the
 * underlying consumer polling thread, and as such, implementations need <i>not</i> be
 * <i>thread-safe</i> nor <i>thread-compatible</i>.
 */
public interface PollStrategy {

    /**
     * Creates a strategy that allows the Kafka consumer to poll from all assigned and un-paused
     * partitions using the default consumer behavior.
     */
    static PollStrategy natural() {
        return new Natural();
    }

    /**
     * Creates "balanced" polling strategy that aims to poll about half of assigned partitions on
     * each polling cycle. The selection of partitions is based on "binary striding", which first
     * starts by selecting every other partition, then the complement of that selection, followed
     * by doubling the "block size" (from 1 to 2) and repeating the process to select every other
     * two-consecutive elements, followed by complement, then doubling again such as to select
     * every other four-consecutive elements, and so on. This process re-loops if/when doubling the
     * block size would result in a block that is greater than the number of permitted partitions.
     *
     * @see io.atleon.util.Collecting#binaryStrides(Collection, Supplier)
     */
    static PollStrategy binaryStrides() {
        return new BinaryStrides();
    }

    /**
     * Creates a polling strategy that prioritizes selecting partitions with the highest lag in
     * units of the polling batch size.
     *
     * @see PollSelectionContext#currentBatchLag(Set, long)
     */
    static PollStrategy greatestBatchLag() {
        return new GreatestBatchLag();
    }

    /**
     * Called when partitions become permissible for polling. This allows the strategy to update
     * its internal state to include the newly permitted partitions.
     *
     * @param partitions the collection of partitions that are now permitted for polling
     */
    default void onPollingPermitted(Collection<TopicPartition> partitions) {

    }

    /**
     * Called when partitions are no longer permissible for polling. This allows the strategy to
     * update its internal state to exclude the prohibited partitions.
     *
     * @param partitions the collection of partitions that are now prohibited from polling
     */
    default void onPollingProhibited(Collection<TopicPartition> partitions) {

    }

    /**
     * Prepares the consumer for the next poll operation by selecting which partitions should be
     * polled based on the strategy's algorithm. This method is called before each consumer poll
     * operation, and it is <i>highly recommended</i> that <i>some</i> selection of partitions is
     * made.
     *
     * @param context the context providing access to partition selection and consumer metadata
     */
    void prepareForPoll(PollSelectionContext context);

    final class Natural implements PollStrategy {

        private Natural() {

        }

        @Override
        public void prepareForPoll(PollSelectionContext context) {
            context.selectNaturally();
        }
    }

    final class BinaryStrides implements PollStrategy {

        private final SortedSet<TopicPartition> sortedPartitions =
            new TreeSet<>(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));

        private List<Set<TopicPartition>> selections = Collections.emptyList();

        private int cycle = 0;

        private BinaryStrides() {

        }

        @Override
        public void onPollingPermitted(Collection<TopicPartition> partitions) {
            if (sortedPartitions.addAll(partitions)) {
                reset();
            }
        }

        @Override
        public void onPollingProhibited(Collection<TopicPartition> partitions) {
            if (sortedPartitions.removeAll(partitions)) {
                reset();
            }
        }

        @Override
        public void prepareForPoll(PollSelectionContext context) {
            if (sortedPartitions.size() <= 1) {
                context.selectNaturally();
            } else {
                context.selectExclusively(selections.get(cycle));
                cycle = (cycle + 1) % selections.size();
            }
        }

        private void reset() {
            selections = Collecting.binaryStrides(sortedPartitions, LinkedHashSet::new);
            cycle %= selections.size();
        }
    }

    final class GreatestBatchLag implements PollStrategy {

        private final Set<TopicPartition> permittedPartitions = new LinkedHashSet<>();

        private GreatestBatchLag() {

        }

        @Override
        public void onPollingPermitted(Collection<TopicPartition> partitions) {
            permittedPartitions.addAll(partitions);
        }

        @Override
        public void onPollingProhibited(Collection<TopicPartition> partitions) {
            permittedPartitions.removeAll(partitions);
        }

        @Override
        public void prepareForPoll(PollSelectionContext context) {
            if (permittedPartitions.size() <= 1) {
                context.selectNaturally();
            } else {
                Map<TopicPartition, Long> lag = context.currentBatchLag(permittedPartitions, Long.MAX_VALUE);
                context.selectExclusively(Collecting.greatest(permittedPartitions, lag::get, HashSet::new));
            }
        }
    }
}
