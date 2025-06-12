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

    public static Scheduler newSingleForReception(String subtype, String clientId) {
        String threadNamePrefix = String.format("atleon-kafka-receive-%s-%s", subtype, clientId);
        return Schedulers.newSingle(new WorkerThreadFactory(threadNamePrefix));
    }

    public static boolean isCurrentThreadFromKafka() {
        return Thread.currentThread() instanceof WorkerThread;
    }

    private static void handleUncaughtException(Thread thread, Throwable exception) {
        LOGGER.error("WorkerThread in group {} failed with an uncaught exception",
            thread.getThreadGroup().getName(), exception);
    }

    private static final class WorkerThread extends Thread {

        public WorkerThread(Runnable target, String name) {
            super(target, name);
            setUncaughtExceptionHandler(KafkaSchedulers::handleUncaughtException);
        }
    }

    private static final class WorkerThreadFactory implements ThreadFactory {

        private static final AtomicLong COUNTER = new AtomicLong();

        private final String threadNamePrefix;

        public WorkerThreadFactory(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            return new WorkerThread(runnable, threadNamePrefix + "-" + COUNTER.incrementAndGet());
        }
    }
}
