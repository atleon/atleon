package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

final class SequencedOffset {

    private final OffsetAndMetadata offsetAndMetadata;

    private final long sequence;;

    public SequencedOffset(OffsetAndMetadata offsetAndMetadata, long sequence) {
        this.offsetAndMetadata = offsetAndMetadata;
        this.sequence = sequence;
    }

    public long rawOffset() {
        return offsetAndMetadata.offset();
    }

    public OffsetAndMetadata offsetAndMetadata() {
        return offsetAndMetadata;
    }

    public long sequence() {
        return sequence;
    }
}
