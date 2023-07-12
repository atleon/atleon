package io.atleon.schemaregistry.confluent;

import io.atleon.schema.SchematicPreSerializer;
import io.atleon.schema.SchematicSerializer;
import io.atleon.util.Configurable;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Function;

/**
 * A serializer that registers serialization schemas with a Schema Registry and includes
 * identifying information about that schema in the first few bytes of serialized payloads.
 *
 * @param <T> The type of data serialized by this serializer
 * @param <S> The type of schema describing serialized objects
 */
public abstract class RegistrySerializer<T, S> extends RegistrySerDe implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistrySerializer.class);

    private boolean autoRegisterSchema = RegistrySerDeConfig.AUTO_REGISTER_SCHEMAS_DEFAULT;

    private boolean useLatestVersion = RegistrySerDeConfig.USE_LATEST_VERSION_DEFAULT;

    private boolean latestCompatibilityStrict = RegistrySerDeConfig.LATEST_COMPATIBILITY_STRICT_DEFAULT;

    @Override
    public void configure(RegistrySerDeConfig config) {
        super.configure(config);
        this.autoRegisterSchema = config.autoRegisterSchema();
        this.useLatestVersion = config.useLatestVersion();
        this.latestCompatibilityStrict = config.getLatestCompatibilityStrict();
    }

    /**
     * Serialize the provided data, using the name of the associated schema as the subject name for
     * registration.
     *
     * @param data The data to be serialized
     * @return Serialized data bytes
     */
    public final byte[] serialize(T data) {
        return serialize(data, ParsedSchema::name);
    }

    /**
     * Serialize the provided data, using provided subject name for registration.
     *
     * @param subject The name of the subject to use for registration
     * @param data    The data to be serialized
     * @return Serialized data bytes
     */
    public final byte[] serialize(String subject, T data) {
        return serialize(data, __ -> subject);
    }

    /**
     * Serialize the provided data, providing the topic and key arguments to the configured
     * {@link io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy} to produce
     * subject names for registration.
     *
     * @param topic The topic being serialized to
     * @param isKey Whether the provided data is key or value data
     * @param data  The data to be serialized
     * @return Serialized data bytes
     */
    public final byte[] serialize(String topic, boolean isKey, T data) {
        return serialize(data, parsedSchema -> getSubjectName(topic, isKey, data, parsedSchema));
    }

    protected final byte[] serialize(T data, Function<ParsedSchema, String> toSubjectName) {
        try {
            SchematicPreSerializer<S> preSerializer = (schema, stream) -> preSerialize(schema, toSubjectName, stream);
            return data == null ? null : serializer().serialize(data, preSerializer).bytes();
        } catch (RegistrationException e) {
            LOGGER.warn("Error registering Schema", e.getCause());
            throw new SerializationException("Error registering Schema", e.getCause());
        }
    }

    protected abstract SchematicSerializer<T, S> serializer();

    protected S preSerialize(S schema, Function<ParsedSchema, String> toSubjectName, ByteArrayOutputStream outputStream) {
        RegisteredSchema registeredSchema = registerOrGet(schema, toSubjectName);
        outputStream.write(MAGIC_BYTE);
        registeredSchema.writeIdTo(outputStream);
        return registeredSchema.extractTypedRawSchema((Class<? extends S>) schema.getClass());
    }

    protected final RegisteredSchema registerOrGet(S schema, Function<ParsedSchema, String> toSubjectName) {
        try {
            ParsedSchema parsedSchema = toParsedSchema(schema);
            String subject = toSubjectName.apply(parsedSchema);
            return registerOrGet(subject, parsedSchema);
        } catch (IOException | RestClientException e) {
            throw new RegistrationException(e);
        }
    }

    protected abstract ParsedSchema toParsedSchema(S schema);

    private RegisteredSchema registerOrGet(String subject, ParsedSchema schema) throws IOException, RestClientException {
        if (autoRegisterSchema) {
            int schemaId = register(subject, schema);
            return new RegisteredSchema(schemaId, schema);
        } else if (useLatestVersion) {
            ParsedSchema latestSchema = lookupLatestVersion(subject, schema, latestCompatibilityStrict);
            int schemaId = schemaRegistry.getId(subject, latestSchema);
            return new RegisteredSchema(schemaId, latestSchema);
        } else {
            int schemaId = schemaRegistry.getId(subject, schema);
            return new RegisteredSchema(schemaId, schema);
        }
    }

    protected static final class RegisteredSchema {

        private final int id;

        private final ParsedSchema parsedSchema;

        private RegisteredSchema(int id, ParsedSchema parsedSchema) {
            this.id = id;
            this.parsedSchema = parsedSchema;
        }

        public void writeIdTo(ByteArrayOutputStream outputStream) {
            outputStream.write(toByte(id >> 24));
            outputStream.write(toByte(id >> 16));
            outputStream.write(toByte(id >> 8));
            outputStream.write(toByte(id));
        }

        public <S> S extractTypedRawSchema(Class<? extends S> rawSchemaType) {
            return ParsedSchemas.extractTypedRawSchema(parsedSchema, rawSchemaType);
        }

        private static byte toByte(int value) {
            return (byte) (value & 0xff);
        }
    }

    private static final class RegistrationException extends RuntimeException {

        public RegistrationException(Throwable cause) {
            super(cause);
        }
    }
}
