package io.atleon.core;

import java.util.function.Function;

public abstract class StringHashGroupExtractor<T> implements Function<T, Integer> {

    private final int modulus;

    public StringHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    public static <T> StringHashGroupExtractor<T> composed(Function<? super T, String> extractor, int modulus) {
        return new Composed<>(extractor, modulus);
    }

    @Override
    public Integer apply(T t) {
        return Murmur3.hashIntoBucket(extractString(t), modulus);
    }

    protected abstract String extractString(T t);

    private static final class Composed<T> extends StringHashGroupExtractor<T> {

        private final Function<? super T, String> extractor;

        public Composed(Function<? super T, String> extractor, int modulus) {
            super(modulus);
            this.extractor = extractor;
        }

        @Override
        protected String extractString(T t) {
            return extractor.apply(t);
        }
    }
}
