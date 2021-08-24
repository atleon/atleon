package io.atleon.core;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

final class AcknowledgingCollection<T> extends AbstractCollection<Alo<T>> {

    private final Collection<T> collection;

    private final Collection<T> unacknowledged;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    private final AloFactory<T> factory;

    public AcknowledgingCollection(Collection<T> collection, Runnable acknowledger, Consumer<? super Throwable> nacknowledger, AloFactory<T> factory) {
        this.collection = collection;
        this.unacknowledged = collection.stream().collect(Collectors.toCollection(() -> Collections.newSetFromMap(new IdentityHashMap<>(collection.size()))));
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
        this.factory = factory;
    }

    @Override
    public String toString() {
        return "AcknowledgingCollection(" + Objects.toString(collection) + ")";
    }

    @Override
    public Iterator<Alo<T>> iterator() {
        Iterator<T> iterator = collection.iterator();
        return new Iterator<Alo<T>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Alo<T> next() {
                return wrap(iterator.next());
            }
        };
    }

    @Override
    public int size() {
        return collection.size();
    }

    private Alo<T> wrap(T value) {
        return factory.create(value, () -> {
            synchronized (unacknowledged) {
                if (unacknowledged.remove(value) && unacknowledged.isEmpty()) {
                    acknowledger.run();
                }
            }
        }, error -> {
            synchronized (unacknowledged) {
                if (unacknowledged.contains(value)) {
                    unacknowledged.clear();
                    nacknowledger.accept(error);
                }
            }
        });
    }
}
