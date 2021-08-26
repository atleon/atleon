package io.atleon.core;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;

public abstract class UuidHashGroupExtractor<T> implements Function<T, Integer> {

    private final HashFunction hashFunction = Hashing.murmur3_32();

    private final int modulus;

    public UuidHashGroupExtractor(int modulus) {
        this.modulus = modulus;
    }

    public static <T> UuidHashGroupExtractor<T> composed(Function<? super T, UUID> uuidExtractor, int modulus) {
        return new Composed<>(uuidExtractor, modulus);
    }

    @Override
    public Integer apply(T t) {
        byte[] uuidBytes = extractUuidBytes(t);
        HashCode hash = hashFunction.hashBytes(uuidBytes);
        return Math.abs(hash.asInt() % modulus);
    }

    protected final byte[] extractUuidBytes(T t) {
        UUID uuid = extractUuid(t);
        ByteBuffer uuidBytes = ByteBuffer.allocate(Long.BYTES * 2);
        uuidBytes.putLong(uuid == null ? 0L : uuid.getMostSignificantBits());  // Defend against null UUIDs
        uuidBytes.putLong(uuid == null ? 0L : uuid.getLeastSignificantBits());
        return uuidBytes.array();
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
