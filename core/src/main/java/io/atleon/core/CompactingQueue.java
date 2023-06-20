package io.atleon.core;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

public interface CompactingQueue<T> extends Queue<T> {

    Node<T> addItem(T item);

    T remove();

    T peek();

    boolean isEmpty();

    Iterator<T> iterator();

    interface Node<T> {

        Node<T> getPrevious();

        Node<T> getNext();

        T getItem();

        List<T> tryCompact();

        default boolean isEmpty() {
            return getItem() == null;
        }
    }
}
