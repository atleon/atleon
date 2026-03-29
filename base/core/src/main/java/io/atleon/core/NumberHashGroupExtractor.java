package io.atleon.core;

import java.util.function.Function;

public abstract class NumberHashGroupExtractor<T> implements Function<T, Integer> {

    private final int modulus;

    public NumberHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    public static <T> NumberHashGroupExtractor<T> composed(
            Function<? super T, ? extends Number> extractor, int modulus) {
        return new Composed<>(extractor, modulus);
    }

    @Override
    public Integer apply(T t) {
        return Murmur3.hashIntoBucket(extractNumber(t), modulus);
    }

    protected abstract Number extractNumber(T t);

    private static final class Composed<T> extends NumberHashGroupExtractor<T> {

        private final Function<? super T, ? extends Number> extractor;

        public Composed(Function<? super T, ? extends Number> extractor, int modulus) {
            super(modulus);
            this.extractor = extractor;
        }

        @Override
        protected Number extractNumber(T t) {
            return extractor.apply(t);
        }
    }
}
