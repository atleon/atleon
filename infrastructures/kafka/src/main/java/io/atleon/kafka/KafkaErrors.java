package io.atleon.kafka;

import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * Utility methods concerned with identifying/handling Kafka-specific errors.
 */
final class KafkaErrors {

    private KafkaErrors() {}

    public static boolean isRetriableCommitFailure(Exception exception) {
        return exception instanceof RetriableCommitFailedException || exception instanceof RebalanceInProgressException;
    }

    public static boolean isFatalSendFailure(Exception exception) {
        // All known fatal send failures are a subset of fatal Producer errors, which will result
        // in the underlying Producer being closed. Upon closure, all following errors will be
        // of type IllegalStateException indicating closed state, so this can be our only check.
        return exception instanceof IllegalStateException;
    }

    public static boolean isFatalProducerException(Throwable error) {
        return error instanceof ProducerFencedException
                || error instanceof OutOfOrderSequenceException
                || error instanceof AuthenticationException
                || error instanceof UnsupportedVersionException
                || error instanceof UnsupportedForMessageFormatException;
    }
}
