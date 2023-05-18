package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import io.atleon.schema.SchematicPreSerializer;
import io.atleon.schema.SchematicSerializer;
import io.atleon.util.Throwing;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.function.Supplier;

/**
 * This Serializer implements serialization-time Avro Schema loading such that Schemas for written
 * data types do not need to be generated/provided prior to runtime. In addition to supporting
 * plain data types and Avro-native data types, this Serializer also supports usage with generic
 * data types via Schema generation based on values populated at serialization time.
 *
 * <p>It should be noted that usage of Schema generation should be combined with some form of
 * caching, as generating schemas is a heavy duty process, and certain usages (like with Schema
 * Registry) enforce a cap on the number of used schemas (by Object identity). Usage with generic
 * data types can complicate this restriction if it is possible for serialized generic types to
 * change during the course of an application's lifetime. In all other cases when schemas are
 * otherwise stable on a per-type basis, this class provides a cache that can be enabled to
 * minimize the number of generated schemas.
 */
public abstract class AvroSerializer<T> implements SchematicSerializer<T, Schema> {

    private final AvroSchemaCache<Class<?>> schemaCache = new AvroSchemaCache<>();

    protected final boolean schemaCachingEnabled;

    protected final boolean schemaGenerationEnabled;

    public AvroSerializer() {
        this(false, false);
    }

    protected AvroSerializer(boolean schemaCachingEnabled, boolean schemaGenerationEnabled) {
        this.schemaCachingEnabled = schemaCachingEnabled;
        this.schemaGenerationEnabled = schemaGenerationEnabled;
    }

    @Override
    public final SchemaBytes<Schema> serialize(T data, SchematicPreSerializer<Schema> preSerializer) {
        try {
            return serializeUnsafe(data, preSerializer);
        } catch (Exception e) {
            throw Throwing.propagate(e);
        }
    }

    protected final SchemaBytes<Schema>
    serializeUnsafe(T data, SchematicPreSerializer<Schema> preSerializer) throws IOException {
        Schema schema = schemaCachingEnabled ? schemaCache.load(data.getClass(), __ -> loadSchema(data)) : loadSchema(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Schema writerSchema = preSerializer.apply(schema, outputStream);
        createDatumWriter(writerSchema).write(data, EncoderFactory.get().directBinaryEncoder(outputStream, null));
        return SchemaBytes.serialized(writerSchema, outputStream.toByteArray());
    }

    protected final Schema loadSchema(T data) {
        Supplier<Schema> supplier = () -> schemaGenerationEnabled ? generateSchema(data) : loadTypeSchema(data.getClass());
        return AvroSchemas.getOrSupply(data, supplier);
    }

    protected final Schema generateSchema(T data) {
        return AvroSerialization.generateWriterSchema(data, this::loadTypeSchema);
    }

    protected abstract Schema loadTypeSchema(Type dataType);

    protected abstract DatumWriter<T> createDatumWriter(Schema schema);
}
