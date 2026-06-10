package io.atleon.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

/**
 * Utilities for encoding and decoding metadata associated with committed Kafka offsets. Such
 * metadata is a compact, string-valued field.
 *
 * <p>For ahead-of-commit encoding, offsets are serialized to a binary format and then
 * Base64-encoded for storage. Within that binary format, a sequence of offsets is written as a
 * version, a count, and the offsets themselves expressed as deltas relative to the committed
 * (anchor) offset, rather than as absolute values. Consecutive deltas of equal magnitude are
 * further compressed via run-length encoding, and each integral value is written as an unsigned
 * variable-length long so that small numbers occupy fewer bytes. A CRC checksum, seeded with the
 * anchor offset, is appended so that decoding can detect corruption or a mismatched anchor and
 * fail safely.
 */
final class OffsetMetadataEncoding {

    private static final byte VERSION = 1;

    private static final byte RUN_MARKER = 0;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder().withoutPadding();

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetMetadataEncoding.class);

    private OffsetMetadataEncoding() {}

    public static String encodeProcessed(long commitOffset, SortedSet<Long> processedOffsets, int limit) {
        return BASE64_ENCODER.encodeToString(serializeProcessed(commitOffset, processedOffsets, limit));
    }

    public static List<Long> decodeProcessed(long assignmentOffset, String metadata, int limit) {
        try {
            return deserializeProcessed(assignmentOffset, Base64.getDecoder().decode(metadata), limit);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid Base64 offset metadata: {}", metadata);
            return Collections.emptyList();
        }
    }

    public static byte[] serializeProcessed(long commitOffset, SortedSet<Long> processedOffsets, int limit) {
        int size = Math.min(processedOffsets.size(), limit);
        return size <= 0 ? EMPTY_BYTES : serializeProcessedUnsafe(commitOffset, processedOffsets, size);
    }

    public static List<Long> deserializeProcessed(long assignmentOffset, byte[] metadata, int limit) {
        try {
            return metadata.length == 0
                    ? Collections.emptyList()
                    : deserializeProcessedUnsafe(assignmentOffset, metadata, limit);
        } catch (Exception e) {
            LOGGER.warn("Failed to deserialize processed offsets from metadata", e);
            return Collections.emptyList();
        }
    }

    private static byte[] serializeProcessedUnsafe(long commitOffset, SortedSet<Long> processedOffsets, int limit) {
        long firstProcessedOffset = processedOffsets.first();
        if (commitOffset > firstProcessedOffset) {
            throw new IllegalArgumentException("Processed offsets must be at least commit offset");
        }

        // Initialize with version and count of offsets that will be serialized
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        CheckedOutputStream checkedStream = new CheckedOutputStream(byteStream, initializeChecksum(commitOffset));
        writeByte(checkedStream, VERSION);
        writeUnsignedVarLong(checkedStream, limit);

        // Write all delta-runs while keeping checksum updated
        long previousOffset = commitOffset - 1;
        long currentRunDelta = firstProcessedOffset - previousOffset;
        long currentRunLength = 0;
        for (Iterator<Long> iterator = processedOffsets.iterator(); iterator.hasNext() && limit > 0; limit--) {
            long offset = iterator.next();
            long delta = offset - previousOffset;
            if (delta <= 0) {
                throw new IllegalArgumentException("Processed offsets must be monotonically increasing");
            } else if (delta == currentRunDelta) {
                currentRunLength++;
            } else {
                flushDeltaOrRun(checkedStream, currentRunDelta, currentRunLength);
                currentRunDelta = delta;
                currentRunLength = 1;
            }
            previousOffset = offset;
        }
        flushDeltaOrRun(checkedStream, currentRunDelta, currentRunLength);

        // Include checksum at end for deserialization validation
        writeLeastSignificantInt(byteStream, checkedStream.getChecksum().getValue());

        return byteStream.toByteArray();
    }

    private static List<Long> deserializeProcessedUnsafe(long assignmentOffset, byte[] metadata, int limit) {
        // Validate checksum first for fail-fast behavior
        ByteBuffer buffer = ByteBuffer.wrap(metadata, metadata.length - Integer.BYTES, Integer.BYTES);
        Checksum expectedChecksum = initializeChecksum(assignmentOffset);
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

        // Read number of offsets that have been encoded
        long remaining = readUnsignedVarLong(buffer);
        List<Long> values = new ArrayList<>(Math.min(limit, Math.toIntExact(remaining)));

        // Read all delta-runs while calculating effective offsets
        long previousOffset = assignmentOffset - 1;
        while (remaining > 0) {
            long deltaOrMarker = readUnsignedVarLong(buffer);

            boolean running = deltaOrMarker == RUN_MARKER;
            long delta = running ? readUnsignedVarLong(buffer) : deltaOrMarker;
            long length = running ? readUnsignedVarLong(buffer) : 1;

            remaining -= length;
            while (values.size() < limit && length-- > 0) {
                values.add(previousOffset += delta);
            }
        }

        // Validate full consumption
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer not fully consumed");
        }

        return values;
    }

    private static void flushDeltaOrRun(OutputStream out, long delta, long count) {
        if (count == 1) {
            writeUnsignedVarLong(out, delta);
        } else if (count == 2) {
            writeUnsignedVarLong(out, delta);
            writeUnsignedVarLong(out, delta);
        } else {
            writeUnsignedVarLong(out, RUN_MARKER);
            writeUnsignedVarLong(out, delta);
            writeUnsignedVarLong(out, count);
        }
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

    private static Checksum initializeChecksum(long offset) {
        Checksum checksum = new CRC32();
        update(checksum, offset);
        return checksum;
    }

    private static void update(Checksum checksum, long value) {
        checksum.update((int) (value >>> 56));
        checksum.update((int) (value >>> 48));
        checksum.update((int) (value >>> 40));
        checksum.update((int) (value >>> 32));
        checksum.update((int) (value >>> 24));
        checksum.update((int) (value >>> 16));
        checksum.update((int) (value >>> 8));
        checksum.update((int) value);
    }
}
