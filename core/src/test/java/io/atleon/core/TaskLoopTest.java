package io.atleon.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskLoopTest {

    @Test
    public void schedule_givenTaskToPerformAsynchronously_expectsExecutionOnAnotherThread() {
        TaskLoop taskLoop = TaskLoop.start("test");

        CompletableFuture<Thread> executionThread = new CompletableFuture<>();
        taskLoop.schedule(() -> executionThread.complete(Thread.currentThread()));

        assertNotEquals(Thread.currentThread(), executionThread.join());

        taskLoop.disposeSafely();
    }

    @Test
    public void schedule_givenTaskThrowsUncaughtException_expectsContinuedExecutionOfSubsequentTasks() {
        TaskLoop taskLoop = TaskLoop.start("test");

        taskLoop.schedule(() -> {
            throw new UnsupportedOperationException("Boom");
        });

        CompletableFuture<Void> completed = new CompletableFuture<>();
        assertTrue(taskLoop.schedule(() -> completed.complete(null)));

        assertNull(completed.join());
    }

    @Test
    public void disposeSafely_givenSchedulingAfterDisposal_expectsRejectedExecution() {
        TaskLoop taskLoop = TaskLoop.start("test");

        assertTrue(taskLoop.schedule(() -> {}));

        taskLoop.disposeSafely();

        assertFalse(taskLoop.schedule(() -> {}));
    }

    @Test
    public void disposeSafely_givenExecutionFromVariousThreads_expectsCorrectDeterminationOfThreadSource() {
        TaskLoop taskLoop1 = TaskLoop.start("test");
        TaskLoop taskLoop2 = TaskLoop.start("test");

        CompletableFuture<Boolean> result1 = new CompletableFuture<>();
        CompletableFuture<Boolean> result2 = new CompletableFuture<>();
        taskLoop1.schedule(() -> result1.complete(taskLoop1.isSourceOfCurrentThread()));
        taskLoop2.schedule(() -> result2.complete(taskLoop1.isSourceOfCurrentThread()));

        assertTrue(result1.join());
        assertFalse(result2.join());
        assertFalse(taskLoop1.isSourceOfCurrentThread());
        assertFalse(taskLoop2.isSourceOfCurrentThread());

        taskLoop1.disposeSafely();
        taskLoop2.disposeSafely();
    }
}
