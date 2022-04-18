package io.atleon.polling;

import java.util.Optional;

public class ComposedPolled<P, O> implements Polled<P, O> {

    private final P payload;
    private final O offset;

    public ComposedPolled(final P payload,
                          final O offset) {
        this.payload = payload;
        this.offset = offset;
    }

    @Override
    public P getPayload() {
        return payload;
    }

    @Override
    public O getOffset() {
        return offset;
    }

    @Override
    public Optional<?> getGroup() {
        return Optional.empty();
    }
}
