package io.atleon.core;

import io.atleon.util.Configurable;

import java.util.List;
import java.util.Map;

/**
 * Observer interface that can be applied to {@link AloQueueingTransformer} to monitor for group
 * queue lifecycle and state.
 */
public interface AloQueueListener extends Configurable {

    static AloQueueListener noOp() {
        return new NoOp();
    }

    static AloQueueListener combine(List<AloQueueListener> listeners) {
        return listeners.size() == 1 ? listeners.get(0) : new Composite(listeners);
    }

    default void configure(Map<String, ?> properties) {}

    /**
     * Callback for when a queue has been created
     *
     * @param group The group for which a queue has been created
     */
    default void created(Object group) {}

    /**
     * Callback for when a number of items in a group has been enqueued
     *
     * @param group The group under which items have been enqueued
     * @param count The number of items that have been enqueued
     */
    default void enqueued(Object group, long count) {}

    /**
     * Callback for when a number of items in a group has been dequeued
     *
     * @param group The group under which items have been dequeued
     * @param count The number of items that have been dequeued
     */
    default void dequeued(Object group, long count) {}

    /**
     * Callback for when the resource managing queues has been disposed
     */
    default void close() {}

    class NoOp implements AloQueueListener {

        private NoOp() {}
    }

    class Composite implements AloQueueListener {

        private final List<AloQueueListener> listeners;

        private Composite(List<AloQueueListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void configure(Map<String, ?> properties) {
            listeners.forEach(listener -> listener.configure(properties));
        }

        @Override
        public void created(Object group) {
            listeners.forEach(listener -> listener.created(group));
        }

        @Override
        public void enqueued(Object group, long count) {
            listeners.forEach(listener -> listener.enqueued(group, count));
        }

        @Override
        public void dequeued(Object group, long count) {
            listeners.forEach(listener -> listener.dequeued(group, count));
        }

        @Override
        public void close() {
            listeners.forEach(AloQueueListener::close);
        }
    }
}
