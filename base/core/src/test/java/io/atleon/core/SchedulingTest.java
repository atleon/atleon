package io.atleon.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchedulingTest {

    @Test
    @EnabledForJreRange(max = JRE.JAVA_20)
    public void newVirtualThreadScheduler_givenJreBeforeJava21_expectsUnsupportedOperationException() {
        UnsupportedOperationException error =
                assertThrows(UnsupportedOperationException.class, () -> Scheduling.newVirtualThreadScheduler("test"));

        assertInstanceOf(ReflectiveOperationException.class, error.getCause());
    }

    @Test
    @EnabledForJreRange(min = JRE.JAVA_21)
    public void newVirtualThreadScheduler_givenJre21OrAfter_expectsTasksExecutedOnNamedVirtualThreads()
            throws Exception {
        Scheduler scheduler = Scheduling.newVirtualThreadScheduler("test");
        try {
            CompletableFuture<Thread> executedOn = new CompletableFuture<>();
            scheduler.schedule(() -> executedOn.complete(Thread.currentThread()));

            Thread thread = executedOn.get(10, TimeUnit.SECONDS);
            assertEquals("test-1", thread.getName());
            assertTrue(Boolean.class.cast(Thread.class.getMethod("isVirtual").invoke(thread)));
        } finally {
            scheduler.dispose();
        }

        assertTrue(scheduler.isDisposed());
    }
}
