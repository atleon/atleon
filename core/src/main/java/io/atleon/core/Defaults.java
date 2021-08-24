package io.atleon.core;

public final class Defaults {

    public static final int PREFETCH = 256;

    public static final int CONCURRENCY = 2048;

    public static final int THREAD_CAP = 10 * Runtime.getRuntime().availableProcessors();

    private Defaults() {

    }
}
