package io.atleon.context;

import io.atleon.core.Alo;
import io.atleon.core.AloDecorator;

/**
 * An {@link AloDecorator} that decorates {@link Alo} elements with
 * {@link io.atleon.context.AloContext AloContext} activation
 *
 * @param <T> The type of data item exposed by decorated {@link Alo}s
 */
public class ContextActivatingAloDecorator<T> implements AloDecorator<T> {

    @Override
    public int order() {
        return OUTERMOST_ORDER - 3000;
    }

    @Override
    public final Alo<T> decorate(Alo<T> alo) {
        return ContextActivatingAlo.create(alo);
    }
}
