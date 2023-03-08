package io.atleon.core;

public class TestAloDecorator<T> implements AloDecorator<T> {
    @Override
    public Alo<T> decorate(Alo<T> alo) {
        return alo;
    }
}
