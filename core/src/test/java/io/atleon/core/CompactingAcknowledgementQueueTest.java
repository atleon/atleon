package io.atleon.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompactingAcknowledgementQueueTest {

    private AcknowledgementQueue compactingAcknowledgementQueue;
    private AcknowledgementQueue orderManagingAcknowledgementQueue;


    @BeforeEach
    public void init() {
        compactingAcknowledgementQueue = CompactingAcknowledgementQueue.create();
        orderManagingAcknowledgementQueue = OrderManagingAcknowledgementQueue.create();
    }

    @Test
    public void testPerfComparison() throws Exception {
        long compaction = runTest(compactingAcknowledgementQueue);
        long standard = runTest(orderManagingAcknowledgementQueue);
        assertTrue(compaction < standard);
        System.out.println("Compaction = " + (compaction/1000000.0) + "ms, Standard = " + (standard/1000000.0) + "ms");
    }

    private long runTest(final AcknowledgementQueue queue) throws Exception {
        final int sampleSize = 100_000;

        ExecutorService executorService = Executors.newFixedThreadPool(100);

        final AtomicBoolean[] acks = new AtomicBoolean[sampleSize];
        for(int i = 0; i < sampleSize; i++) {
            acks[i] = new AtomicBoolean(false);
        }

        final AcknowledgementQueue.InFlight[] tasks = new AcknowledgementQueue.InFlight[sampleSize];
        for(int i = 0; i < sampleSize; i++) {
            final int j = i;
            tasks[i] = queue.add(() -> acks[j].set(true), error -> {});
        }

        List<Future<Long>> futures = new ArrayList<>();
        for(int i = 1; i < sampleSize; i++) {
            final int j = i;
            futures.add(executorService.submit(() -> queue.complete(tasks[j])));
        }
        long count = 0;
        for(Future<Long> f : futures) {
            count += f.get();
        }
        long start = System.nanoTime();
        count += queue.complete(tasks[0]);
        long end = System.nanoTime();
        assertEquals(sampleSize, count);
        return end - start;
    }
}
