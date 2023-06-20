package io.atleon.core;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

public class ConcurrentCompactingQueue<T> extends AbstractQueue<T> implements CompactingQueue<T> {

    private static final int HOPS = 2;
    private final AtomicReferenceFieldUpdater<ConcurrentCompactingQueue, ConcurrentCompactingQueue.CompactingNode> HEAD =
            AtomicReferenceFieldUpdater.newUpdater(ConcurrentCompactingQueue.class, CompactingNode.class, "head");
    private final AtomicReferenceFieldUpdater<ConcurrentCompactingQueue, ConcurrentCompactingQueue.CompactingNode> TAIL =
            AtomicReferenceFieldUpdater.newUpdater(ConcurrentCompactingQueue.class, CompactingNode.class, "tail");
    private final AtomicReferenceFieldUpdater<ConcurrentCompactingQueue.CompactingNode, ConcurrentCompactingQueue.CompactingNode> PREV =
            AtomicReferenceFieldUpdater.newUpdater(CompactingNode.class, CompactingNode.class, "previous");
    private final AtomicReferenceFieldUpdater<ConcurrentCompactingQueue.CompactingNode, ConcurrentCompactingQueue.CompactingNode> NEXT =
            AtomicReferenceFieldUpdater.newUpdater(CompactingNode.class, CompactingNode.class, "next");

    private final AtomicReferenceFieldUpdater<ConcurrentCompactingQueue.CompactingNode, Object> ITEM =
            AtomicReferenceFieldUpdater.newUpdater(CompactingNode.class, Object.class, "item");

    private AtomicInteger size;

    private final CompactingNode NEXT_TERMINATOR = new CompactingNode();
    private final CompactingNode PREV_TERMINATOR = new CompactingNode();

    private final Function<T, Boolean> compactCheck;

    volatile CompactingNode head;

    volatile CompactingNode tail;

    ConcurrentCompactingQueue() {
        this(null);
    }

    ConcurrentCompactingQueue(final Function<T, Boolean> compactCheck) {
        this.compactCheck = compactCheck;
        head = tail = new CompactingNode();
        size = new AtomicInteger(0);
    }

    @Override
    public Node<T> addItem(T item) {
        final CompactingNode node = new CompactingNode(item);
        add(node);
        size.incrementAndGet();
        return node;
    }

    @Override
    public boolean offer(T t) {
        addItem(t);
        return true;
    }

    @Override
    public T remove() {
        restart: for (;;) {
            for (CompactingNode first = findFirst(), p = first;;) {
                final T item;
                if ((item = p.getItem()) != null) {
                    if (first.previous != null) continue restart;
                    if (ITEM.compareAndSet(p, item, null)) {
                        remove(p);
                        return item;
                    }
                }
                if (p == (p = p.next)) continue restart;
                if (p == null) {
                    if (first.previous != null) continue restart;
                    return null;
                }
            }
        }
    }

    @Override
    public T poll() {
        return remove();
    }

    @Override
    public T peek() {
        return peekInternal().item;
    }

    @Override
    public boolean isEmpty() {
        final Node<T> h = peekInternal();
        return h == null || h.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return new CompactingIterator();
    }

    @Override
    public int size() {
        return size.get();
    }

    private CompactingNode findFirst() {
        while (true) {
            CompactingNode h = head;
            CompactingNode p = h;
            while (p.previous != null && p.previous.previous != null) {
                p = (h == head) ? h : p.previous.previous;
            }
            if (p == h || HEAD.compareAndSet(this, h, p)) {
               return p;
            }
        }
    }

    private CompactingNode peekInternal() {
        CompactingNode p;
        do {
            p = findFirst();
            while (p != null && p.isEmpty() && p != p.next) {
                p = p.next;
            }
        } while(p != null && p.isEmpty());
        return p;
    }

    private void add(final CompactingNode node) {
        CompactingNode t;
        CompactingNode p;
        do {
            t = tail;
            p = t;
            while (p.next != null) {
                p = p.next;
            }
            if (p.previous != p) {
                PREV.set(node, p);
                if (NEXT.compareAndSet(p, null, node)) {
                    if (p != t) {
                        TAIL.weakCompareAndSet(this, t, node);
                    }
                    return;
                }
            }
        } while (p.next != null || p.previous == p);
    }

    private void remove(CompactingNode node) {
        if (node.previous == null) {
            removeFirst(node, node.next);
        } else if (node.next == null) {
            removeLast(node, node.previous);
        } else {
            CompactingNode activePred, activeSucc;
            boolean isFirst, isLast;
            int hops = 1;

            // Find active predecessor

            for (CompactingNode p = node.previous; ; ++hops) {
                if (!node.previous.isEmpty()) {
                    activePred = p;
                    isFirst = false;
                    break;
                }
                if (p.previous == null) {
                    if (p.next == p)
                        return;
                    activePred = p;
                    isFirst = true;
                    break;
                } else if (p == p.previous) {
                    return;
                } else {
                    p = p.previous;
                }
            }

            // Find active successor
            for (CompactingNode p = node.next; ; ++hops) {
                if (!p.isEmpty()) {
                    activeSucc = p;
                    isLast = false;
                    break;
                }
                CompactingNode q = p.next;
                if (q == null) {
                    if (p.previous == p)
                        return;
                    activeSucc = p;
                    isLast = true;
                    break;
                }
                else if (p == q)
                    return;
                else
                    p = q;
            }

            if (hops < HOPS && (isFirst || isLast))
                return;

            skipDeletedSuccessors(activePred);
            skipDeletedPredecessors(activeSucc);

            if ((isFirst | isLast) &&

                    (activePred.next == activeSucc) &&
                    (activeSucc.previous == activePred) &&
                    (isFirst ? activePred.previous == null : activePred.item != null) &&
                    (isLast  ? activeSucc.next == null : activeSucc.item != null)) {

                updateHead();
                updateTail();

                PREV.set(node, isFirst ? PREV_TERMINATOR : node);
                NEXT.set(node, isLast  ? NEXT_TERMINATOR : node);
            }
        }
    }

    private void removeFirst(CompactingNode first,
                             CompactingNode next) {
        CompactingNode n = next;
        while (n.next != null && n.next != n.next.next) {
            if (!n.next.isEmpty() || n.next.next == null) {
                if (n.previous != n && NEXT.compareAndSet(first, next, n.next)) {
                    skipDeletedPredecessors(n.next);
                    if (first.previous == null
                            && (n.next.next == null || !n.next.isEmpty())
                            && n.next.previous == first) {

                        updateHead();
                        updateTail();

                        NEXT.set(n, n);
                        PREV.set(n, PREV_TERMINATOR);
                    }
                } else {
                    n = n.next;
                }
            }
        }
    }

    private void removeLast(CompactingNode last,
                            CompactingNode prev) {
        CompactingNode p = prev;
        while(p.previous != null && p.previous != p.previous.previous) {
            if (!p.previous.isEmpty() || p.previous.previous == null) {
                if (p.next != p && PREV.compareAndSet(last, prev, p.previous)) {
                    skipDeletedSuccessors(p.previous);
                    if (last.next == null
                            && (last.previous.previous == null || !p.previous.isEmpty())
                            && p.previous.next == last) {

                        updateHead();
                        updateTail();

                        PREV.set(p, p);
                        NEXT.set(p, NEXT_TERMINATOR);
                    }
                } else {
                    p = p.previous;
                }
            }
        }
    }

    private void skipDeletedSuccessors(CompactingNode node) {
        do {
            CompactingNode next = node.next;
            CompactingNode p = next;
            while (p.isEmpty() && p != p.next) {
                if (p.next == null) {
                    break;
                } else {
                    p = p.next;
                }
            }

            if (!(p == p.next || p == p.previous)
                    && (next == p || NEXT.compareAndSet(node, next, p)))
                return;

        } while (!node.isEmpty() || node.previous == null);
    }

    private void skipDeletedPredecessors(CompactingNode node) {
        do {
            CompactingNode prev = node.previous;
            CompactingNode p = prev;
            while (p.isEmpty() && p != p.previous) {
                CompactingNode q = p.previous;
                if (p.previous == null) {
                    break;
                } else {
                    p = q;
                }
            }

            if (!(p == p.previous || p == p.next)
                    && (prev == p || PREV.compareAndSet(node, prev, p)))
                return;

        } while (!node.isEmpty() || node.next == null);
    }

    private void updateHead() {
        CompactingNode h, p;
        restart:
        while ((h = head).isEmpty() && (p = head.previous) != null) {
            while (true) {
                if (p.previous == null || (p = p.previous).previous == null) {
                    if (HEAD.compareAndSet(this, h, p)) {
                        return;
                    }
                } else if (h != head) {
                    continue restart;
                } else {
                    p = p.previous;
                }
            }
        }
    }

    private void updateTail() {
        CompactingNode t, p;
        restart:
        while ((t = tail).isEmpty() && (p = tail.next) != null) {
            while (true) {
                if (p.next == null || (p = p.next).next == null) {
                    if (TAIL.compareAndSet(this, t, p)) {
                        return;
                    }
                } else if (t != tail) {
                    continue restart;
                } else {
                    p = p.next;
                }
            }
        }
    }

    private CompactingNode getSuccessor(CompactingNode p) {
        return p == p.next ? findFirst() : p.next;
    }

    private Boolean getShouldCompact(final T item) {
        return compactCheck != null && item != null && compactCheck.apply(item);
    }

    final class CompactingNode implements Node<T> {

        volatile T item;

        volatile CompactingNode previous;

        volatile CompactingNode next;

        CompactingNode() {
            this(null);
        }

        CompactingNode(final T item) {
            this.item = item;
        }

        @Override
        public Node<T> getPrevious() {
            return previous;
        }

        @Override
        public Node<T> getNext() {
            return next;
        }

        @Override
        public T getItem() {
            return item;
        }

        @Override
        public List<T> tryCompact() {
            List<T> compacted = new ArrayList<>();
            if (previous != null && !previous.isEmpty()) {
                final List<T> pc;
                if (!(pc = previous.tryCompact()).isEmpty()) {
                    compacted.addAll(pc);
                }
                final T previousItem = previous.item;
                if (getShouldCompact(previousItem) && ITEM.compareAndSet(previous, previousItem, null)) {
                    remove(previous);
                    compacted.add(previousItem);
                }
            }
            return compacted;
        }
    }

    final class CompactingIterator implements Iterator<T> {

        private CompactingNode nextNode;

        private T nextItem;

        private CompactingNode lastNode;

        CompactingIterator() {
            advance();
        }

        private void advance() {
            lastNode = nextNode;

            CompactingNode p = (nextNode == null) ? findFirst() : getSuccessor(nextNode);
            while (true) {
                if (p == null || !p.isEmpty()) {
                    nextNode = p;
                    nextItem = (p == null) ? null : p.item;
                    break;
                }
                p = getSuccessor(p);
            }
        }

        public boolean hasNext() {
            return nextItem != null;
        }

        public T next() {
            T item = nextItem;
            if (item == null) {
                throw new NoSuchElementException();
            }
            advance();
            return item;
        }

        public void remove() {
            CompactingNode l = lastNode;
            if (l == null) {
                throw new IllegalStateException();
            }
            l.item = null;
            ConcurrentCompactingQueue.this.remove(l);
            lastNode = null;
        }
    }
}
