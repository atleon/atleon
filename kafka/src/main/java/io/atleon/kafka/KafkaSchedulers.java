package io.atleon.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class for creating {@link Scheduler} instances for usage with Kafka resources.
 */
final class KafkaSchedulers {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSchedulers.class);

    private static final AtomicLong THREAD_COUNTER = new AtomicLong();

    public static Scheduler newSingleForReceiving(String subtype, String clientId) {
        String threadNamePrefix = String.format("atleon-kafka-receive-%s-%s", subtype, clientId);
        return Schedulers.newSingle(newWorkerThreadFactory(threadNamePrefix));
    }

    public static Scheduler newSingleForSending(String subtype, String clientId) {
        String threadNamePrefix = String.format("atleon-kafka-send-%s-%s", subtype, clientId);
        return Schedulers.newSingle(newWorkerThreadFactory(threadNamePrefix));
    }

    public static boolean isCurrentThreadFromKafka() {
        return Thread.currentThread() instanceof WorkerThread;
    }

    private static ThreadFactory newWorkerThreadFactory(String threadNamePrefix) {
        return runnable -> new WorkerThread(runnable, threadNamePrefix + "-" + THREAD_COUNTER.incrementAndGet());
    }

    private static void handleUncaughtException(Thread thread, Throwable exception) {
        ThreadGroup threadGroup = thread.getThreadGroup();
        String threadGroupName = threadGroup == null ? "<N/A>" : threadGroup.getName();
        LOGGER.error("WorkerThread in group {} failed with an uncaught exception", threadGroupName, exception);
    }

    private static final class WorkerThread extends Thread {

        public WorkerThread(Runnable target, String name) {
            super(target, name);
            setUncaughtExceptionHandler(KafkaSchedulers::handleUncaughtException);
        }
    }
}
