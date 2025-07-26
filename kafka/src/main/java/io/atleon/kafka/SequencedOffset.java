package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

/**
 * An {@link OffsetAndMetadata} that is associated with some sequence number.
 */
final class SequencedOffset {

    private final OffsetAndMetadata offsetAndMetadata;

    private final long sequence;

    public SequencedOffset(OffsetAndMetadata offsetAndMetadata, long sequence) {
        this.offsetAndMetadata = offsetAndMetadata;
        this.sequence = sequence;
    }

    public OffsetAndMetadata offsetAndMetadata() {
        return offsetAndMetadata;
    }

    public long sequence() {
        return sequence;
    }
}
