package io.atleon.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Encapsulates a serialized task loop in which executions can be scheduled with thread safety.
 */
public final class TaskLoop {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskLoop.class);

    private static final AtomicLong THREAD_COUNTER = new AtomicLong();

    private static final ThreadLocal<TaskLoop> SOURCE = new ThreadLocal<>();

    private final String name;

    private final Consumer<Runnable> runner;

    private final ExecutorService taskService;

    private TaskLoop(String name, Consumer<Runnable> runner) {
        this.name = name;
        this.runner = runner;
        this.taskService = Executors.newSingleThreadExecutor(this::newThread);
    }

    public static TaskLoop start(String name) {
        return start(name, Runnable::run);
    }

    public static TaskLoop start(String name, Consumer<Runnable> runner) {
        return new TaskLoop(name, runner);
    }

    public boolean schedule(Runnable task) {
        try {
            taskService.execute(() -> runner.accept(task));
            return true;
        } catch (RejectedExecutionException e) {
            LOGGER.warn("Execution rejected in TaskLoop with name={}", name);
            return false;
        }
    }

    public void disposeSafely() {
        try {
            taskService.shutdownNow();
        } catch (Throwable error) {
            LOGGER.error("Failed to shut down taskService", error);
        }
    }

    public boolean isSourceOfCurrentThread() {
        return SOURCE.get() == this;
    }

    private Thread newThread(Runnable runnable) {
        Runnable decoratedRunnable = () -> {
            SOURCE.set(this);
            runnable.run();
        };
        Thread thread = new Thread(decoratedRunnable, name + "-" + THREAD_COUNTER.incrementAndGet());
        thread.setUncaughtExceptionHandler(TaskLoop::handleUncaughtException);
        return thread;
    }

    private static void handleUncaughtException(Thread thread, Throwable exception) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        String threadGroupName = threadGroup == null ? "<N/A>" : threadGroup.getName();
        LOGGER.error("Thread in group {} failed with an uncaught exception", threadGroupName, exception);
    }
}
