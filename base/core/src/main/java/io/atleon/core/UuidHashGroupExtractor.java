package io.atleon.core;

import java.util.UUID;
import java.util.function.Function;

public abstract class UuidHashGroupExtractor<T> implements Function<T, Integer> {

    private final int modulus;

    public UuidHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    public static <T> UuidHashGroupExtractor<T> composed(Function<? super T, UUID> uuidExtractor, int modulus) {
        return new Composed<>(uuidExtractor, modulus);
    }

    @Override
    public Integer apply(T t) {
        return Murmur3.hashIntoBucket(extractUuid(t), modulus);
    }

    protected abstract UUID extractUuid(T t);

    private static final class Composed<T> extends UuidHashGroupExtractor<T> {

        private final Function<? super T, UUID> extractor;

        public Composed(Function<? super T, UUID> extractor, int modulus) {
            super(modulus);
            this.extractor = extractor;
        }

        @Override
        protected UUID extractUuid(T t) {
            return extractor.apply(t);
        }
    }
}
