package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Encapsulates a serialized task loop in which executions can be scheduled and serialized with
 * thread safety.
 */
final class TaskLoop {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskLoop.class);

    private static final AtomicLong THREAD_COUNTER = new AtomicLong();

    private final String name;

    private final Scheduler scheduler;

    private final Disposable disposable;

    private final Sinks.Many<Runnable> tasks = Sinks.unsafe().many().unicast().onBackpressureError();

    private final SerialQueue<Runnable> taskQueue = SerialQueue.onEmitNext(tasks);

    private TaskLoop(String name, Consumer<Runnable> runner) {
        this.name = name;
        this.scheduler = Schedulers.newSingle(this::newThread);
        this.disposable = tasks.asFlux()
            .publishOn(scheduler, Integer.MAX_VALUE)
            .subscribe(runner);
    }

    public static TaskLoop startWithErrorHandling(String name, Consumer<Throwable> errorHandler) {
        return new TaskLoop(name, it -> runSafely(it, errorHandler));
    }

    public void schedule(Runnable task) {
        taskQueue.addAndDrain(task);
    }

    public void disposeSafely() {
        runSafely(disposable::dispose, "disposable::dispose");
        runSafely(scheduler::dispose, "scheduler::dispose");
    }

    public boolean isSourceOfCurrentThread() {
        return Thread.currentThread().getName().startsWith(name);
    }

    private Thread newThread(Runnable runnable) {
        return new WorkerThread(runnable, name + "-" + THREAD_COUNTER.incrementAndGet());
    }

    private static void runSafely(Runnable task, String name) {
        runSafely(task, error -> LOGGER.error("Unexpected failure: name={}", name, error));
    }

    private static void runSafely(Runnable task, java.util.function.Consumer<Throwable> errorHandler) {
        try {
            task.run();
        } catch (Throwable e) {
            errorHandler.accept(e);
        }
    }

    private static void handleUncaughtException(Thread thread, Throwable exception) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        String threadGroupName = threadGroup == null ? "<N/A>" : threadGroup.getName();
        LOGGER.error("WorkerThread in group {} failed with an uncaught exception", threadGroupName, exception);
    }

    private static final class WorkerThread extends Thread {

        public WorkerThread(Runnable target, String name) {
            super(target, name);
            setUncaughtExceptionHandler(TaskLoop::handleUncaughtException);
        }
    }
}
