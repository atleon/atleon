package io.atleon.kafka;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates information about a machine, including an ID, and when this data was
 * created/"birthed". Provides convenience methods for serializing and deserializing as a
 * {@link ByteBuffer}.
 */
final class MachineData {

    private final UUID id;

    private final long birthTime;

    MachineData(UUID id, long birthTime) {
        this.id = id;
        this.birthTime = birthTime;
    }

    /**
     * Create new {@link MachineData} with a random ID and birth time of now
     */
    public static MachineData birth() {
        return new MachineData(UUID.randomUUID(), System.currentTimeMillis());
    }

    public static MachineData fromByteBuffer(ByteBuffer buffer) {
        long idMostSignificantBits = extractLong(buffer);
        long idLeastSignificantBits = extractLong(buffer);
        long birthTime = extractLong(buffer);
        return new MachineData(new UUID(idMostSignificantBits, idLeastSignificantBits), birthTime);
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES * 3);
        byteBuffer.putLong(id.getMostSignificantBits());
        byteBuffer.putLong(id.getLeastSignificantBits());
        byteBuffer.putLong(birthTime);
        byteBuffer.rewind();
        return byteBuffer;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MachineData that = (MachineData) o;
        return birthTime == that.birthTime && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, birthTime);
    }

    @Override
    public String toString() {
        return "MachineData{id=" + id + ", birthTime=" + birthTime + '}';
    }

    public UUID id() {
        return id;
    }

    public long birthTime() {
        return birthTime;
    }

    private static Long extractLong(ByteBuffer byteBuffer) {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        longBuffer.putLong(byteBuffer.getLong());
        Buffer.class.cast(longBuffer).flip(); // Need explicit Buffer cast to be JRE backward-compatible
        return longBuffer.getLong();
    }
}
