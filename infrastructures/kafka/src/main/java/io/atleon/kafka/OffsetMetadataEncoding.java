package io.atleon.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

/**
 * Encodes a set of {@link Offsets} into a compact, Base64-encoded {@link String} suitable for use
 * as Kafka offset commit metadata, and decodes it back again. Offsets are stored relative to a
 * base offset (typically decremented commit offset) as a sequence of "delta-runs" (a delta paired
 * with a repetition count), keeping the encoding proportional to the number of distinct gaps
 * rather than the number of offsets.
 *
 * <p>The serialized layout is {@code [version][offset count][delta-runs][checksum]}, where
 * deltas and counts are written as unsigned variable-length long values, and a trailing CRC32
 * checksum (seeded with the base offset) guards against corruption and base-offset mismatch.
 *
 * <p>Encoding is bounded by {@code maxSizeOnWrite}: if the offsets do not fit, the leading
 * delta-runs that do fit are retained and the remainder are truncated. {@link #decode(long, String)}
 * is lenient, returning empty {@link Offsets} (rather than throwing) on any malformed, corrupt, or
 * version-incompatible input.
 */
final class OffsetMetadataEncoding {

    private static final byte VERSION = 1;

    private static final byte RUN_MARKER = 0;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder().withoutPadding();

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetMetadataEncoding.class);

    private final int maxSizeOnWrite;

    public OffsetMetadataEncoding(int maxSizeOnWrite) {
        this.maxSizeOnWrite = maxSizeOnWrite;
    }

    public String encode(long baseOffset, Offsets offsets) {
        // Take Base64 expansion into account when calculating max serialization bytes
        int maxBytes = maxSizeOnWrite < Integer.MAX_VALUE ? (int) ((long) maxSizeOnWrite * 3 / 4) : Integer.MAX_VALUE;
        return BASE64_ENCODER.encodeToString(serialize(baseOffset, offsets, maxBytes));
    }

    public Offsets decode(long baseOffset, String metadata) {
        try {
            return deserializeUnsafe(baseOffset, Base64.getDecoder().decode(metadata));
        } catch (Exception e) {
            LOGGER.warn("Failed to deserialize offsets from metadata", e);
            return new Offsets();
        }
    }

    private byte[] serialize(long baseOffset, Offsets offsets, int maxBytes) {
        if (offsets.isEmpty()) {
            return EMPTY_BYTES;
        }

        // In order to calculate the maximum bytes available to write delta-runs, we start with the
        // total byte limit, and then subtract:
        // 1. One for byte needed for version
        // 2. Pathological bytes needed to represent offset count
        // 3. Byte count needed to represent checksum as integer
        // We then serialize as many delta-runs as possible while fitting within remaining byte
        // limit. If none fit, we log and return an empty result.
        int maxBytesForDeltaRuns = maxBytes - 1 - varLongSizeOf(offsets.size()) - Integer.BYTES;
        DeltaRuns deltaRuns = serializeDeltaRuns(baseOffset, offsets, maxBytesForDeltaRuns);
        if (deltaRuns.totalCount() <= 0) {
            LOGGER.warn("Offset delta-runs cannot be represented with maxBytes: {}", maxBytes);
            return EMPTY_BYTES;
        }

        // Realize serialization format: [version][offset count][delta-runs][checksum]
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        CheckedOutputStream checkedStream = new CheckedOutputStream(byteStream, initializeChecksum(baseOffset));
        writeByte(checkedStream, VERSION);
        writeUnsignedVarLong(checkedStream, deltaRuns.totalCount());
        deltaRuns.writeBytesTo(checkedStream);
        writeLeastSignificantInt(byteStream, checkedStream.getChecksum().getValue());

        return byteStream.toByteArray();
    }

    private DeltaRuns serializeDeltaRuns(long baseOffset, Offsets offsets, int maxBytes) {
        DeltaRuns deltaRuns = new DeltaRuns(maxBytes);
        long previousOffset = baseOffset;
        long currentRunDelta = 0;
        int currentRunLength = 0;
        PrimitiveIterator.OfLong offsetIterator = offsets.longIterator();
        while (offsetIterator.hasNext()) {
            long offset = offsetIterator.nextLong();
            long delta = offset - previousOffset;
            if (delta <= 0) {
                throw new IllegalArgumentException("Offsets must monotonically increase from base offset");
            } else if (delta == currentRunDelta) {
                currentRunLength++;
            } else if (deltaRuns.flush(currentRunDelta, currentRunLength)) {
                currentRunDelta = delta;
                currentRunLength = 1;
            } else {
                return deltaRuns;
            }
            previousOffset = offset;
        }
        deltaRuns.flush(currentRunDelta, currentRunLength);
        return deltaRuns;
    }

    private Offsets deserializeUnsafe(long baseOffset, byte[] metadata) {
        Offsets.Builder builder = new Offsets.Builder();
        if (metadata.length > 0) {
            deserializeUnsafe(baseOffset, metadata, builder::addNext);
        }
        return builder.build();
    }

    private void deserializeUnsafe(long baseOffset, byte[] metadata, LongConsumer consumer) {
        // Validate checksum first for fail-fast behavior
        ByteBuffer buffer = ByteBuffer.wrap(metadata, metadata.length - Integer.BYTES, Integer.BYTES);
        Checksum expectedChecksum = initializeChecksum(baseOffset);
        expectedChecksum.update(metadata, 0, buffer.position());
        if (readLeastSignificantInt(buffer) != expectedChecksum.getValue()) {
            throw new IllegalArgumentException("Checksum validation failed");
        }

        // Initialize data deserialization with version check
        buffer.position(0).limit(metadata.length - Integer.BYTES);
        byte version = buffer.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Incompatible version in offset commit metadata: " + version);
        }

        // Read number of encoded offsets and all delta-runs while consuming effective offsets
        long remaining = readUnsignedVarLong(buffer);
        long previousOffset = baseOffset;
        while (remaining > 0) {
            long value = readUnsignedVarLong(buffer);

            boolean running = value == RUN_MARKER;
            long delta = running ? readUnsignedVarLong(buffer) : value;
            long length = running ? readUnsignedVarLong(buffer) : 1;

            remaining -= length;
            while (length > 0) {
                consumer.accept(previousOffset += delta);
                length--;
            }
        }

        // Validate full consumption
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer not fully consumed");
        }
    }

    private static Checksum initializeChecksum(long offset) {
        Checksum checksum = new CRC32();
        checksum.update((int) (offset >>> 56));
        checksum.update((int) (offset >>> 48));
        checksum.update((int) (offset >>> 40));
        checksum.update((int) (offset >>> 32));
        checksum.update((int) (offset >>> 24));
        checksum.update((int) (offset >>> 16));
        checksum.update((int) (offset >>> 8));
        checksum.update((int) offset);
        return checksum;
    }

    private static int varLongSizeOf(long value) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10);
        writeUnsignedVarLong(outputStream, value);
        return outputStream.size();
    }

    private static void writeUnsignedVarLong(OutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            writeByte(out, (int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte(out, (int) value);
    }

    private static long readUnsignedVarLong(ByteBuffer buffer) {
        long value = 0;
        for (int shift = 0; shift < 64 && buffer.hasRemaining(); shift += 7) {
            byte nextByte = buffer.get();
            value |= (long) (nextByte & 0x7F) << shift;
            if ((nextByte & 0x80) == 0) {
                return value;
            }
        }
        throw new IllegalArgumentException("Malformed VarLong");
    }

    private static void writeLeastSignificantInt(OutputStream out, long value) {
        writeByte(out, (int) value >>> 24);
        writeByte(out, (int) value >>> 16);
        writeByte(out, (int) value >>> 8);
        writeByte(out, (int) value);
    }

    private static long readLeastSignificantInt(ByteBuffer buffer) {
        int value = 0;
        value |= (buffer.get() & 0xFF) << 24;
        value |= (buffer.get() & 0xFF) << 16;
        value |= (buffer.get() & 0xFF) << 8;
        value |= (buffer.get() & 0xFF);
        return Integer.toUnsignedLong(value);
    }

    private static void writeByte(OutputStream out, int value) {
        try {
            out.write(value);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write byte to OutputStream", e);
        }
    }

    private static void writeBytes(OutputStream out, byte[] value, int length) {
        try {
            out.write(value, 0, length);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write bytes to OutputStream", e);
        }
    }

    private static final class DeltaRuns {

        private final int maxBytes;

        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        private int totalCount = 0;

        private int stopIndex = 0;

        public DeltaRuns(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        public boolean flush(long delta, int count) {
            if (count == 1) {
                writeUnsignedVarLong(bytes, delta);
            } else if (count == 2) {
                writeUnsignedVarLong(bytes, delta);
                writeUnsignedVarLong(bytes, delta);
            } else if (count >= 3) {
                writeUnsignedVarLong(bytes, RUN_MARKER);
                writeUnsignedVarLong(bytes, delta);
                writeUnsignedVarLong(bytes, count);
            }

            if (bytes.size() <= maxBytes) {
                totalCount += count;
                stopIndex = bytes.size();
                return true;
            } else {
                LOGGER.warn("Offset delta-runs are being truncated");
                return false;
            }
        }

        public int totalCount() {
            return totalCount;
        }

        public void writeBytesTo(OutputStream outputStream) {
            writeBytes(outputStream, bytes.toByteArray(), stopIndex);
        }
    }
}
