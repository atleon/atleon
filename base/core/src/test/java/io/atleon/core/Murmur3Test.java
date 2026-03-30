package io.atleon.core;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Murmur3Test {

    private static final int BUCKET_COUNT = 128;

    @Test
    void hashIntoBucket_givenNullNumber_expectsZero() {
        assertEquals(0, Murmur3.hashIntoBucket((Number) null, BUCKET_COUNT));
    }

    @ParameterizedTest
    @MethodSource("numberTestCases")
    void hashIntoBucket_givenNumber_expectsCorrectBucket(long number, int expectedBucket) {
        assertEquals(expectedBucket, Murmur3.hashIntoBucket(number, BUCKET_COUNT));

        HashFunction hashFunction = Hashing.murmur3_32();
        assertEquals(expectedBucket, Math.floorMod(hashFunction.hashLong(number).asInt(), BUCKET_COUNT));
    }

    @Test
    void hashIntoBucket_givenNullString_expectsZero() {
        assertEquals(0, Murmur3.hashIntoBucket((String) null, BUCKET_COUNT));
    }

    @ParameterizedTest
    @MethodSource("stringTestCases")
    void hashIntoBucket_givenString_expectsCorrectBucket(String string, int expectedBucket) {
        assertEquals(expectedBucket, Murmur3.hashIntoBucket(string, BUCKET_COUNT));

        HashFunction hashFunction = Hashing.murmur3_32();
        assertEquals(
                expectedBucket,
                Math.floorMod(hashFunction.hashUnencodedChars(string).asInt(), BUCKET_COUNT));
    }

    @Test
    void hashIntoBucket_givenNullUuid_expectsZero() {
        assertEquals(0, Murmur3.hashIntoBucket((UUID) null, BUCKET_COUNT));
    }

    @ParameterizedTest
    @MethodSource("uuidTestCases")
    void hashIntoBucket_givenUuid_expectsCorrectBucket(UUID uuid, int expectedBucket) {
        assertEquals(expectedBucket, Murmur3.hashIntoBucket(uuid, BUCKET_COUNT));

        HashFunction hashFunction = Hashing.murmur3_32();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES * 2]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        assertEquals(
                expectedBucket,
                Math.floorMod(hashFunction.hashBytes(byteBuffer.array()).asInt(), BUCKET_COUNT));
    }

    // Generated examples from Guava
    private static Stream<Arguments> numberTestCases() {
        return Stream.of(
                Arguments.of(0L, 124),
                Arguments.of(1L, 68),
                Arguments.of(-1L, 104),
                Arguments.of(Long.MAX_VALUE, 63),
                Arguments.of(Long.MIN_VALUE, 37),
                Arguments.of(-5025562857975149833L, 48),
                Arguments.of(-5843495416241995736L, 89),
                Arguments.of(5694868678511409995L, 103),
                Arguments.of(5111195811822994797L, 51),
                Arguments.of(-6169532649852302182L, 62),
                Arguments.of(-1782466964123969572L, 76),
                Arguments.of(6802844026563419272L, 2),
                Arguments.of(5086654115216342560L, 17),
                Arguments.of(8552898714322622292L, 26),
                Arguments.of(-4004755535478349341L, 114),
                Arguments.of(-1488139573943419793L, 121),
                Arguments.of(8051837266862454915L, 11),
                Arguments.of(-4613416830416070574L, 7),
                Arguments.of(7130900098642117381L, 93),
                Arguments.of(3272055439932147596L, 113));
    }

    // Generated examples from Guava
    private static Stream<Arguments> stringTestCases() {
        return Stream.of(
                Arguments.of("", 0),
                Arguments.of("a", 26),
                Arguments.of("ab", 74),
                Arguments.of("abc", 67),
                Arguments.of("Hello, World!", 33),
                Arguments.of("Awrn1eZzy1t", 6),
                Arguments.of("LzmzDvJXEgGIQF1pT91s1Q6Q", 86),
                Arguments.of("nJ0r7HB94SBZ0wfc71NW", 89),
                Arguments.of("t7gzZ1j1c", 119),
                Arguments.of("K6CwpZLlkvAeITTpsArQt0kKZUM", 92),
                Arguments.of("n3mg0kWWerozNLTj3ira17d", 86),
                Arguments.of("nhrt1cjy3cg", 27),
                Arguments.of("Q9wwzrfwW1qgb82jaArib", 5),
                Arguments.of("LKvmp0DYouh1VtI9c02TTE7qw", 67),
                Arguments.of("l", 89),
                Arguments.of("6Cf1jpLvX", 41),
                Arguments.of("px", 99),
                Arguments.of("Y9SDNfKB8lPExauwtoIDJL4f", 2),
                Arguments.of("X9bN", 71),
                Arguments.of("hFjf0FmyAP5tHCEy", 41));
    }

    // Generated examples from Guava
    private static Stream<Arguments> uuidTestCases() {
        return Stream.of(
                Arguments.of(UUID.fromString("00000000-0000-0000-0000-000000000000"), 120),
                Arguments.of(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 110),
                Arguments.of(UUID.fromString("dce20b69-20f0-bd7e-b274-b53d66eb0cfe"), 72),
                Arguments.of(UUID.fromString("28fe5454-0301-961c-ccd0-ca16850ba065"), 111),
                Arguments.of(UUID.fromString("b9b70ff7-6ecb-a8df-cb7c-e8c8c3cb6ad3"), 65),
                Arguments.of(UUID.fromString("d2bfeab4-a47f-4b25-d9ac-2d366c6ab179"), 95),
                Arguments.of(UUID.fromString("fc2798ca-88d0-3fbd-210e-72413e4fdfd5"), 38),
                Arguments.of(UUID.fromString("9d3551b7-0a4f-1874-8af9-d5d72600556b"), 127),
                Arguments.of(UUID.fromString("27181dd6-98fe-9cef-88d9-f932beebdab3"), 64),
                Arguments.of(UUID.fromString("8c8ada4e-c0d8-4abb-541c-815bf845f154"), 67),
                Arguments.of(UUID.fromString("b85a172f-e790-42a2-6346-9bf1a1277393"), 96),
                Arguments.of(UUID.fromString("03e733d5-721a-3740-1a57-8c2a84e63705"), 74),
                Arguments.of(UUID.fromString("1a965d90-6476-30d5-2c58-84177537a68c"), 74),
                Arguments.of(UUID.fromString("e5cb5486-2f65-3a5b-6b3a-4e2aaf8e8780"), 14),
                Arguments.of(UUID.fromString("d8f31ad7-7113-4bdb-5625-8afc75735e2b"), 49),
                Arguments.of(UUID.fromString("6c56f201-20c5-05fc-0d6a-874a279a257b"), 99),
                Arguments.of(UUID.fromString("9a574a84-f76d-12dc-2a89-ee40606f4df2"), 24),
                Arguments.of(UUID.fromString("7b7389cb-00f6-4022-929f-dbf9480137e1"), 59),
                Arguments.of(UUID.fromString("846dca41-894c-a653-b60f-1445f7ca9cd4"), 112),
                Arguments.of(UUID.fromString("ad27f9cb-0e01-7165-d9e2-61ae44359c50"), 117));
    }
}
