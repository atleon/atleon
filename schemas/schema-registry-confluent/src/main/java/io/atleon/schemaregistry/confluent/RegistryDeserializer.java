package io.atleon.schemaregistry.confluent;

import io.atleon.schema.KeyableSchema;
import io.atleon.schema.SchematicDeserializer;
import io.atleon.util.Configurable;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A deserializer that uses schemas fetched from a Schema Registry to deserialize data. Each
 * payload received should have a schema identifier in the beginning of its data which is used
 * to query the registry for the corresponding schema (and cache it).
 *
 * @param <T> The type of data deserialized by this deserializer
 * @param <S> The type of schema describing deserialized objects
 */
public abstract class RegistryDeserializer<T, S> extends RegistrySerDe implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryDeserializer.class);

    private boolean readNullOnFailure = RegistryDeserializerConfig.READ_NULL_ON_FAILURE_DEFAULT;

    public void configure(RegistryDeserializerConfig config) {
        super.configure(config);
        readNullOnFailure = config.readNullOnFailure();
    }

    /**
     * Deserialize provided bytes into concrete object. This class assumes that valid data contains
     * identifying information about the schema used to serialize the bytes of the payload.
     *
     * @param data A byte array payload to deserialize
     * @return A concrete object that may be null
     */
    public final T deserialize(byte[] data) {
        try {
            return data == null || data.length == 0 ? null : deserializer().deserialize(data, this::fetchWriterSchema);
        } catch (SchemaFetchFailedException e) {
            throw new IllegalStateException("Failed to fetch schema for id: " + e.getSchemaId(), e);
        } catch (RuntimeException e) {
            if (readNullOnFailure) {
                LOGGER.warn("Failed to deserialize message. Returning null", e);
                return null;
            } else {
                LOGGER.warn("Failed to deserialize message.", e);
                throw e;
            }
        }
    }

    protected abstract SchematicDeserializer<T, S> deserializer();

    protected final KeyableSchema<S> fetchWriterSchema(ByteBuffer dataBuffer) {
        int schemaId = extractSchemaId(dataBuffer);
        try {
            ParsedSchema parsedSchema = getSchemaById(schemaId);
            return KeyableSchema.keyed(schemaId, toSchema(parsedSchema));
        } catch (IOException | RestClientException e) {
            throw new SchemaFetchFailedException(schemaId, e);
        }
    }

    protected abstract S toSchema(ParsedSchema parsedSchema);

    private static int extractSchemaId(ByteBuffer dataBuffer) {
        byte firstByte = dataBuffer.get();
        if (firstByte != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }

        if (dataBuffer.remaining() < 4) {
            throw new SerializationException("Data is missing schema ID!");
        } else {
            return dataBuffer.getInt();
        }
    }

    private static final class SchemaFetchFailedException extends RuntimeException {

        private final int schemaId;

        public SchemaFetchFailedException(int schemaId, Throwable cause) {
            super(cause);
            this.schemaId = schemaId;
        }

        public int getSchemaId() {
            return schemaId;
        }
    }
}
