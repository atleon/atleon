package io.atleon.polling;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;

import java.util.function.Consumer;

public class AloPollingRecordFactory<T> implements AloFactory<T> {

    @Override
    public Alo<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowedger) {
        return new ComposedAlo<>(t, acknowledger, nacknowedger);
    }

}
