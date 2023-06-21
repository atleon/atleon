package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OrderManagingAcknowledgementQueueTest {

    @Test
    public void acknowledgementsAreExecutedInOrderOfCreationAfterCompletion() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.create();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicBoolean thirdAcknowledged = new AtomicBoolean();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), error -> {});
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), error -> {});
        queue.add(() -> thirdAcknowledged.set(true), error -> {});

        long drained = queue.complete(secondInFlight);

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertFalse(thirdAcknowledged.get());

        drained = queue.complete(firstInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertTrue(secondAcknowledged.get());
        assertFalse(thirdAcknowledged.get());
    }

    @Test
    public void nacknowledgementCanBeCompletedInOrder() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.create();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), secondNacknowledged::set);

        long drained = queue.completeExceptionally(secondInFlight, new IllegalStateException());

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertNull(secondNacknowledged.get());

        drained = queue.complete(firstInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
    }

    @Test
    public void recompletionOfInFlightsIsIgnored() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.create();

        AtomicInteger firstAcknowledgements = new AtomicInteger();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicInteger secondAcknowledgements = new AtomicInteger();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(firstAcknowledgements::incrementAndGet, firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(secondAcknowledgements::incrementAndGet, secondNacknowledged::set);

        queue.completeExceptionally(secondInFlight, new IllegalStateException());

        assertEquals(0, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertNull(secondNacknowledged.get());

        queue.complete(secondInFlight);

        assertEquals(0, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertNull(secondNacknowledged.get());

        queue.complete(firstInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        queue.complete(firstInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        queue.complete(secondInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
    }

    @Test
    public void executionOnlyHappensOnOneThreadInNonBlockingFashion() throws Exception {
        CompletableFuture<Boolean> firstAcknowledgementStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> firstAcknowledgementAllowed = new CompletableFuture<>();
        Runnable firstAcknowledger = () -> {
            try {
                firstAcknowledgementStarted.complete(true);
                firstAcknowledgementAllowed.get();
            } catch (Exception e) {
                fail("This should never happen");
            }
        };

        CompletableFuture<Boolean> secondAcknowledgementStarted = new CompletableFuture<>();
        Runnable secondAcknowledger = () -> secondAcknowledgementStarted.complete(true);

        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.create();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(firstAcknowledger, error -> {});
        AcknowledgementQueue.InFlight secondInFlight = queue.add(secondAcknowledger, error -> {});

        AtomicLong asyncDrained = new AtomicLong(0L);
        Executors.newSingleThreadExecutor().submit(() -> asyncDrained.set(queue.complete(firstInFlight)));

        assertTrue(firstAcknowledgementStarted.get());

        // Would block indefinitely here if execution blocked since first execution in progress
        long syncDrained = queue.complete(secondInFlight);

        assertEquals(0, syncDrained);

        firstAcknowledgementAllowed.complete(true);

        assertTrue(secondAcknowledgementStarted.get());

        Timing.waitForCondition(() -> asyncDrained.get() != 0);

        assertEquals(2, asyncDrained.get());
    }

    @Test
    public void concurrentAcknowledgementsAndNacknowledgementsExecuteCorrectly() {
        int parallelism = Runtime.getRuntime().availableProcessors() * 8;
        int count = parallelism * 250;
        AtomicInteger positiveCount = new AtomicInteger(0);
        AtomicInteger negativeCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong drained = new AtomicLong();

        AcknowledgementQueue acknowledgementQueue = OrderManagingAcknowledgementQueue.create();

        Flux.range(0, count)
            .map(i -> acknowledgementQueue.add(positiveCount::incrementAndGet, error -> negativeCount.incrementAndGet()))
            .parallel(parallelism)
            .runOn(Schedulers.boundedElastic())
            .subscribe(
                inFlight -> {
                    Timing.pause((long) (50 * Math.random()));
                    if (Math.random() <= .65D) {
                        errorCount.incrementAndGet();
                        IllegalArgumentException error = new IllegalArgumentException("Boom");
                        drained.addAndGet(acknowledgementQueue.completeExceptionally(inFlight, error));
                    } else {
                        drained.addAndGet(acknowledgementQueue.complete(inFlight));
                    }
                }
            );

        Timing.waitForCondition(() -> drained.get() == count, 30000);

        assertEquals(count, drained.get());
        assertEquals(errorCount.get(), negativeCount.get());
        assertEquals(count, positiveCount.get() + negativeCount.get());
    }
}