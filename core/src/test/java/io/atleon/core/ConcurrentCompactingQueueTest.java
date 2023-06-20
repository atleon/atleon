package io.atleon.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConcurrentCompactingQueueTest {

    private ExecutorService executorService;
    private CompactingQueue<String> queue;

    @BeforeEach
    public void init() {
        executorService = Executors.newFixedThreadPool(100);
        queue = new ConcurrentCompactingQueue<>();
    }

    @Test
    public void testConcurrentPuts() throws Exception {
        List<Future<CompactingQueue.Node<String>>> futures = new ArrayList<>();
        for(int i = 0; i < 1000; i++) {
            futures.add(executorService.submit(() -> queue.addItem(UUID.randomUUID().toString())));
        }
        for(Future<CompactingQueue.Node<String>> f : futures) {
            f.get();
        }
        List<String> list = new ArrayList<>();
        while(!queue.isEmpty()) {
            list.add(queue.remove());
        }
        assertEquals(1000, list.size());
    }

    @Test
    public void testConcurrentPutsAndTakes() throws Exception {
        final List<String> list = new ArrayList<>();
        executorService.execute(() -> {
            for(int i = 0; i < 10000; i++) {
                queue.addItem(UUID.randomUUID().toString());
            }
        });
        executorService.submit(() -> {
            try {
                while (list.size() < 10000) {
                    while (!queue.isEmpty()) {
                        list.add(queue.remove());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).get();
        assertEquals(10000, list.size());
    }
}
