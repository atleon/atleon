package io.atleon.core;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * A Collection of {@link Alo} produced from a one-to-many mapping of another Alo. Takes care of
 * acknowledgement propagated from the original source by only executing acknowledgement iff all
 * resultant items are acknowledged OR one of the emitted items is nacknowledged.
 *
 * @param <T> The type of data item contained in Alo items in this Collection
 */
final class AcknowledgingCollection<T> extends AbstractCollection<Alo<T>> {

    private final Alo<Collection<T>> aloCollection;

    private final AloFactory<T> aloFactory;

    private final Set<T> unacknowledged;

    private AcknowledgingCollection(Alo<Collection<T>> aloCollection) {
        this.aloCollection = aloCollection;
        this.aloFactory = aloCollection.propagator();
        this.unacknowledged = copyToIdentityHashSet(aloCollection.get());
    }

    public static <T> Collection<Alo<T>> fromNonEmptyAloCollection(Alo<Collection<T>> aloCollection) {
        return new AcknowledgingCollection<>(aloCollection);
    }

    @Override
    public String toString() {
        return "AcknowledgingCollection(" + aloCollection.get() + ")";
    }

    @Override
    public Iterator<Alo<T>> iterator() {
        Iterator<T> iterator = aloCollection.get().iterator();
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
        return aloCollection.get().size();
    }

    private Alo<T> wrap(T value) {
        return aloFactory.create(value, () -> {
            synchronized (unacknowledged) {
                if (unacknowledged.remove(value) && unacknowledged.isEmpty()) {
                    Alo.acknowledge(aloCollection);
                }
            }
        }, error -> {
            synchronized (unacknowledged) {
                if (unacknowledged.contains(value)) {
                    unacknowledged.clear();
                    Alo.nacknowledge(aloCollection, error);
                }
            }
        });
    }

    private static <T> Set<T> copyToIdentityHashSet(Collection<T> collection) {
        Set<T> set = Collections.newSetFromMap(new IdentityHashMap<>(collection.size()));
        set.addAll(collection);
        return set;
    }
}
