package io.atleon.core;

public final class TestAlo extends GenericAlo<String> {

    public TestAlo(String data) {
        super(data);
    }

    public TestAlo(String data, Runnable acknowledgerHook) {
        super(data, acknowledgerHook);
    }
}
