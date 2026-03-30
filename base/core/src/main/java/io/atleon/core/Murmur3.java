package io.atleon.core;

import org.jspecify.annotations.Nullable;

import java.util.UUID;

/**
 * Implements the MurmurHash3 x86 32-bit hash function with seed zero.
 *
 * @see <a href="https://en.wikipedia.org/wiki/MurmurHash#MurmurHash3">MurmurHash3 (Wikipedia)</a>
 */
final class Murmur3 {

    private static final int KMIX_C1 = 0xcc9e2d51;

    private static final int KMIX_C2 = 0x1b873593;

    private static final int HMIX_ADDEND = 0xe6546b64;

    private static final int FMIX_C1 = 0x85ebca6b;

    private static final int FMIX_C2 = 0xc2b2ae35;

    private Murmur3() {}

    public static int hashIntoBucket(@Nullable Number number, int bucketCount) {
        if (number == null) {
            return 0;
        }
        int hash = 0;
        long longValue = number.longValue();
        hash = mixHash(hash, mixKey((int) longValue));
        hash = mixHash(hash, mixKey((int) (longValue >>> 32)));
        hash = finalizeHash(hash, Long.BYTES);
        return Math.floorMod(hash, bucketCount);
    }

    public static int hashIntoBucket(@Nullable String string, int bucketCount) {
        if (string == null) {
            return 0;
        }
        int hash = 0;
        int index = 0;
        int length = string.length();
        for (; index + 1 < length; index += 2) {
            int key = string.charAt(index) | (string.charAt(index + 1) << 16);
            hash = mixHash(hash, mixKey(key));
        }
        if (index < length) {
            hash ^= mixKey(string.charAt(index));
        }
        hash = finalizeHash(hash, length * Character.BYTES);
        return Math.floorMod(hash, bucketCount);
    }

    public static int hashIntoBucket(@Nullable UUID uuid, int bucketCount) {
        if (uuid == null) {
            return 0;
        }
        int hash;
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        hash = 0;
        hash = mixHash(hash, mixKey(Integer.reverseBytes((int) (msb >>> 32))));
        hash = mixHash(hash, mixKey(Integer.reverseBytes((int) msb)));
        hash = mixHash(hash, mixKey(Integer.reverseBytes((int) (lsb >>> 32))));
        hash = mixHash(hash, mixKey(Integer.reverseBytes((int) lsb)));
        hash = finalizeHash(hash, Long.BYTES * 2);
        return Math.floorMod(hash, bucketCount);
    }

    private static int mixKey(int key) {
        key *= KMIX_C1;
        key = Integer.rotateLeft(key, 15);
        key *= KMIX_C2;
        return key;
    }

    private static int mixHash(int hash, int mixedKey) {
        hash ^= mixedKey;
        hash = Integer.rotateLeft(hash, 13);
        hash = hash * 5 + HMIX_ADDEND;
        return hash;
    }

    private static int finalizeHash(int hash, int inputLength) {
        hash ^= inputLength;
        hash ^= hash >>> 16;
        hash *= FMIX_C1;
        hash ^= hash >>> 13;
        hash *= FMIX_C2;
        hash ^= hash >>> 16;
        return hash;
    }
}
