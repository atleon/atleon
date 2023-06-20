package io.atleon.core;

import java.util.Iterator;

public interface CompactingQueue<T> {

    Node<T> add(T item);

    T remove();

    T peek();

    boolean isEmpty();

    Iterator<T> iterator();

    interface Node<T> {

        T getItem();

        void tryCompact();

        default boolean isEmpty() {
            return getItem() == null;
        }
    }
}
