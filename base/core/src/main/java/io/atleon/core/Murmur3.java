package io.atleon.core;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

final class Murmur3 {

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private Murmur3() {}

    public static int hashIntoBucket(@Nullable Number number, int bucketCount) {
        long longValue = number != null ? number.longValue() : 0L;
        HashCode hash = HASH_FUNCTION.hashLong(longValue);
        return Math.floorMod(hash.asInt(), bucketCount);
    }

    public static int hashIntoBucket(@Nullable String string, int bucketCount) {
        String nonNullString = Objects.toString(string, "");
        HashCode hash = HASH_FUNCTION.hashUnencodedChars(nonNullString);
        return Math.floorMod(hash.asInt(), bucketCount);
    }

    public static int hashIntoBucket(@Nullable UUID uuid, int bucketCount) {
        byte[] uuidBytes = uuid != null ? extractUuidBytes(uuid) : new byte[Long.BYTES * 2];
        HashCode hash = HASH_FUNCTION.hashBytes(uuidBytes);
        return Math.floorMod(hash.asInt(), bucketCount);
    }

    private static byte[] extractUuidBytes(UUID uuid) {
        ByteBuffer uuidBytes = ByteBuffer.allocate(Long.BYTES * 2);
        uuidBytes.putLong(uuid.getMostSignificantBits());
        uuidBytes.putLong(uuid.getLeastSignificantBits());
        return uuidBytes.array();
    }
}
