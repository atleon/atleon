package io.atleon.avro;

import io.atleon.schema.SchemaBytes;
import io.atleon.schema.SchematicPreSerializer;
import io.atleon.schema.SchematicSerializer;
import io.atleon.util.Throwing;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This Serializer implements serialization-time Avro Schema loading such that Schemas for written
 * data types do not need to be generated/provided prior to runtime. In addition to supporting
 * plain data types and Avro-native data types, this Serializer also supports usage with generic
 * data types via Schema generation based on values populated at serialization time.
 *
 * <p>It should be noted that usage of Schema generation should be combined with some form of
 * caching, as generating schemas is a heavy-duty process, and certain usages (like with Schema
 * Registry) enforce a cap on the number of used schemas (by Object identity). Usage with generic
 * data types can complicate this restriction if it is possible for serialized generic types to
 * change during the course of an application's lifetime. In all other cases when schemas are
 * otherwise stable on a per-type basis, this class provides a cache that can be enabled to
 * minimize the number of generated schemas.
 */
public final class AvroSerializer<T> implements SchematicSerializer<T, Schema> {

    private final GenericData genericData;

    private final Function<Type, Schema> typeSchemaLoader;

    private final boolean schemaCachingEnabled;

    private final boolean schemaGenerationEnabled;

    private final boolean removeJavaProperties;

    private final AvroSchemaCache<Class<?>> schemaCache = new AvroSchemaCache<>();

    private final AvroSchemaCache<Schema> transformedSchemas = new AvroSchemaCache<>();

    private AvroSerializer(GenericData genericData, Function<Type, Schema> typeSchemaLoader) {
        this(genericData, typeSchemaLoader, false, false, false);
    }

    private AvroSerializer(
        GenericData genericData,
        Function<Type, Schema> typeSchemaLoader,
        boolean schemaCachingEnabled,
        boolean schemaGenerationEnabled,
        boolean removeJavaProperties
    ) {
        this.genericData = genericData;
        this.typeSchemaLoader = typeSchemaLoader;
        this.schemaCachingEnabled = schemaCachingEnabled;
        this.schemaGenerationEnabled = schemaGenerationEnabled;
        this.removeJavaProperties = removeJavaProperties;
    }

    public static <T> AvroSerializer<T> generic() {
        return create(GenericData.get());
    }

    public static <T> AvroSerializer<T> reflect() {
        return create(AtleonReflectData.get());
    }

    public static <T> AvroSerializer<T> create(GenericData genericData) {
        return new AvroSerializer<>(genericData, GenericDatas.createTypeSchemaLoader(genericData));
    }

    public AvroSerializer<T> withSchemaCachingEnabled(boolean schemaCachingEnabled) {
        return new AvroSerializer<>(
            genericData,
            typeSchemaLoader,
            schemaCachingEnabled,
            schemaGenerationEnabled,
            removeJavaProperties
        );
    }

    public AvroSerializer<T> withSchemaGenerationEnabled(boolean schemaGenerationEnabled) {
        return new AvroSerializer<>(
            genericData,
            typeSchemaLoader,
            schemaCachingEnabled,
            schemaGenerationEnabled,
            removeJavaProperties
        );
    }

    public AvroSerializer<T> withRemoveJavaProperties(boolean removeJavaProperties) {
        return new AvroSerializer<>(
            genericData,
            typeSchemaLoader,
            schemaCachingEnabled,
            schemaGenerationEnabled,
            removeJavaProperties
        );
    }

    @Override
    public SchemaBytes<Schema> serialize(T data, SchematicPreSerializer<Schema> preSerializer) {
        try {
            return serializeUnsafe(data, preSerializer);
        } catch (Exception e) {
            throw Throwing.propagate(e);
        }
    }

    private SchemaBytes<Schema> serializeUnsafe(T data, SchematicPreSerializer<Schema> preSerializer) throws IOException {
        Schema schema = schemaCachingEnabled ? schemaCache.load(data.getClass(), __ -> loadSchema(data)) : loadSchema(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Schema writerSchema = preSerializer.apply(schema, outputStream);
        createDatumWriter(writerSchema).write(data, EncoderFactory.get().directBinaryEncoder(outputStream, null));
        return SchemaBytes.serialized(writerSchema, outputStream.toByteArray());
    }

    private Schema loadSchema(T data) {
        Supplier<Schema> supplier = schemaGenerationEnabled
            ? () -> AvroSerialization.generateWriterSchema(data, typeSchemaLoader)
            : () -> typeSchemaLoader.apply(data.getClass());
        Schema schema = AvroSchemas.getOrSupply(data, supplier);
        return areAnySchemaTransformsEnabled() ? transformSchema(schema) : schema;
    }

    private boolean areAnySchemaTransformsEnabled() {
        return removeJavaProperties;
    }

    private Schema transformSchema(Schema schema) {
        return schemaCachingEnabled // Avoid redundant caching
            ? transformUncachedSchema(schema)
            : transformedSchemas.load(schema, this::transformUncachedSchema);
    }

    private Schema transformUncachedSchema(Schema schema) {
        if (removeJavaProperties) {
            schema = AvroSchemas.removeJavaProperties(schema);
        }
        return schema;
    }

    private DatumWriter<T> createDatumWriter(Schema schema) {
        return (DatumWriter<T>) genericData.createDatumWriter(schema);
    }
}
