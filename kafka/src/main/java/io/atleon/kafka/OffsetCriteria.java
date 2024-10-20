package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Instant;

/**
 * Criteria that describe offsets that may exist in a TopicPartition. All Criteria are "inclusive".
 */
public abstract class OffsetCriteria {

    private OffsetCriteria() {

    }

    /**
     * Points to the first Record's offset in a TopicPartition
     */
    public static OffsetCriteria earliest() {
        return new Earliest();
    }

    /**
     * Points to the last Record's offset in a TopicPartition
     */
    public static OffsetCriteria latest() {
        return new Latest();
    }

    /**
     * Points to the Record's offset which was produced "around" the given timestamp. If used as a
     * min-criteria, points to Record's offset produced at-or-after given timestamp. If used as a
     * max-criteria, points to a Record's offset produced at-or-before given timestamp.
     */
    public static OffsetCriteria timestamp(Instant instant) {
        return new Timestamp(instant.toEpochMilli());
    }

    /**
     * Points to the Record's offset which was produced "around" the given timestamp. If used as a
     * min-criteria, points to Record's offset produced at-or-after given timestamp. If used as a
     * max-criteria, points to a Record's offset produced at-or-before given timestamp.
     */
    public static OffsetCriteria timestamp(long epochMillis) {
        return new Timestamp(epochMillis);
    }

    /**
     * Points to the Record's offset which is "around" where the provided consumer group is
     * committed. If used as a min-criteria, points to the next record that the consumer group
     * would consume. If used as a max-criteria, points to the last record that the group consumed.
     */
    public static OffsetCriteria consumerGroup(String groupId) {
        return new ConsumerGroup(groupId, OffsetResetStrategy.NONE);
    }

    /**
     * Points to the Record's offset which is "around" where the provided consumer group is
     * committed. If used as a min-criteria, points to the next record that the consumer group
     * would consume. If used as a max-criteria, points to the last record that the group consumed.
     * If the group does not have a committed offset, then the provided {@link OffsetResetStrategy}
     * is used to determine where consumption <i>would</i> begin.
     */
    public static OffsetCriteria consumerGroup(String groupId, OffsetResetStrategy resetStrategy) {
        return new ConsumerGroup(groupId, resetStrategy);
    }

    /**
     * Points to Record with given offset. Package private as usage only makes sense when combined
     * with a TopicPartition.
     */
    static OffsetCriteria raw(long offset) {
        return new Raw(offset);
    }

    /**
     * Converts Criteria to a zero-length Range. Only Records that exactly satisfy this Criteria
     * will be returned.
     */
    public OffsetRange asRange() {
        return to(this);
    }

    public OffsetRange to(OffsetCriteria maxInclusive) {
        return OffsetRange.of(this, maxInclusive);
    }

    public static final class Earliest extends OffsetCriteria {

        private Earliest() {

        }
    }

    public static final class Latest extends OffsetCriteria {

        private Latest() {

        }
    }

    public static final class Timestamp extends OffsetCriteria {

        private final long epochMillis;

        private Timestamp(long epochMillis) {
            this.epochMillis = epochMillis;
        }

        public long epochMillis() {
            return epochMillis;
        }
    }

    public static final class ConsumerGroup extends OffsetCriteria {

        private final String groupId;

        private final OffsetResetStrategy resetStrategy;

        private ConsumerGroup(String groupId, OffsetResetStrategy resetStrategy) {
            this.groupId = groupId;
            this.resetStrategy = resetStrategy;
        }

        public String groupId() {
            return groupId;
        }

        public OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    public static final class Raw extends OffsetCriteria {

        private final long offset;

        private Raw(long offset) {
            this.offset = offset;
        }

        public long offset() {
            return offset;
        }
    }
}
