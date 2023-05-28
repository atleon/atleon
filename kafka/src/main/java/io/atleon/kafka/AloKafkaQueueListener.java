package io.atleon.kafka;

import io.atleon.core.AloQueueListener;

/**
 * Kafka-specific extension of {@link AloQueueListener}. This is useful as a marker interface to
 * make auto-loading of listeners explicit.
 */
public interface AloKafkaQueueListener extends AloQueueListener {

}
