package io.atleon.core;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Atleon-specific utilities for working with {@link Scheduler} and {@link Schedulers} APIs
 */
final class Scheduling {

    private Scheduling() {}

    /**
     * Creates a {@link Scheduler} that executes each submitted task on a new virtual thread. Note
     * that virtual threads are only available on Java 21+, and this method will fail with
     * {@link UnsupportedOperationException} if invoked on any earlier Java version. The returned
     * scheduler does <i>not</i> support delayed or periodic tasks; It is rather more appropriate
     * to use {@link Schedulers#parallel()} for such tasks.
     */
    public static Scheduler newVirtualThreadScheduler(String name) {
        return Schedulers.fromExecutorService(newVirtualThreadExecutorService(name), name);
    }

    private static ExecutorService newVirtualThreadExecutorService(String threadNamePrefix) {
        try {
            Method method = Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class);
            return ExecutorService.class.cast(method.invoke(null, newVirtualThreadFactory(threadNamePrefix)));
        } catch (ReflectiveOperationException e) {
            throw new UnsupportedOperationException("Java 21+ required to create virtual thread executor service", e);
        }
    }

    private static ThreadFactory newVirtualThreadFactory(String threadNamePrefix) {
        try {
            Class<?> builderType = Class.forName("java.lang.Thread$Builder");
            Method nameMethod = builderType.getMethod("name", String.class, long.class);
            Method factoryMethod = builderType.getMethod("factory");

            Object builder = Thread.class.getMethod("ofVirtual").invoke(null);
            builder = nameMethod.invoke(builder, threadNamePrefix + "-", 1L);
            return ThreadFactory.class.cast(factoryMethod.invoke(builder));
        } catch (ReflectiveOperationException e) {
            throw new UnsupportedOperationException("Java 21+ required to create virtual thread factory", e);
        }
    }
}
