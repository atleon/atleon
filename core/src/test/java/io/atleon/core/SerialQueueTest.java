package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerialQueueTest {

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @Test
    public void addingAndDrainingDoesNotBlockConcurrentThreads() throws Exception {
        Sinks.Many<Long> sink = Sinks.many().unicast().onBackpressureError();
        SerialQueue<Long> queue = SerialQueue.onEmitNext(sink);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch permitToProceed = new CountDownLatch(1);
        AtomicInteger finished = new AtomicInteger();

        sink.asFlux()
            .doOnNext(next -> {
                started.countDown();
                try {
                    permitToProceed.await();
                    finished.incrementAndGet();
                } catch (Exception e) {
                    throw new IllegalStateException("Permit was not awaited properly", e);
                }
            })
            .subscribe();

        // Add and drain an item from the queue
        Future<?> future = executorService.submit(() -> queue.addAndDrain(System.currentTimeMillis()));

        // Wait for the Executor to be emitting in to the Flux and its thread to be blocked
        assertTrue(started.await(10, TimeUnit.SECONDS));

        // Add and drain another item from the queue, which should not block since previous thread is emitting
        executorService.submit(() -> queue.addAndDrain(System.currentTimeMillis()))
            .get(10, TimeUnit.SECONDS);

        // Nothing should be finished yet
        assertEquals(0, finished.get());

        // Allow the blocked thread to proceed
        permitToProceed.countDown();

        // Wait for the blocked thread to finish
        future.get(10, TimeUnit.SECONDS);

        assertEquals(2, finished.get());
    }
}