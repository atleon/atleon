package io.atleon.avro;

import io.atleon.util.FieldResolution;
import io.atleon.util.Instantiation;
import io.atleon.util.TypeResolution;
import io.atleon.util.ValueResolution;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AvroDeserialization {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDeserialization.class);

    private AvroDeserialization() {}

    public static Object instantiateReferenceData(Schema writerSchema) {
        Object referenceData = Instantiation.one(ReflectData.get().getClass(writerSchema));
        if (TypeResolution.isGenericClass(referenceData.getClass())) {
            consumeGenericFields(
                    referenceData.getClass(),
                    writerSchema,
                    (genericField, fieldSchema) ->
                            instantiateGenericReferenceDataField(referenceData, genericField, fieldSchema));
        }
        return referenceData;
    }

    public static void consumeGenericFields(
            Class<?> referenceClass, Schema schema, BiConsumer<Field, Schema> genericFieldConsumer) {
        Set<Type> genericParameters = TypeResolution.getAllTypeParameters(referenceClass);
        Map<String, Field> fieldsByName = FieldResolution.getAllFieldsByName(referenceClass);
        for (Schema.Field schemaField : schema.getFields()) {
            Field dataField = fieldsByName.get(schemaField.name());
            if (dataField != null && genericParameters.contains(dataField.getGenericType())) {
                genericFieldConsumer.accept(dataField, schemaField.schema());
            }
        }
    }

    public static Schema generateReaderReferenceSchema(
            Object referenceData, Schema writerSchema, Function<Type, Schema> typeSchemaLoader) {
        return AvroSchemas.isRecord(writerSchema) && TypeResolution.isGenericClass(referenceData.getClass())
                ? generateReaderReferenceRecordSchema(referenceData, writerSchema, typeSchemaLoader)
                : typeSchemaLoader.apply(referenceData.getClass());
    }

    private static void instantiateGenericReferenceDataField(
            Object referenceData, Field genericField, Schema fieldSchema) {
        AvroSchemas.reduceNonNull(fieldSchema)
                .ifPresent(nonNullFieldSchema ->
                        instantiateNonNullGenericReferenceDataField(referenceData, genericField, nonNullFieldSchema));
    }

    private static void instantiateNonNullGenericReferenceDataField(
            Object referenceData, Field genericField, Schema nonNullFieldSchema) {
        try {
            Object fieldData = instantiateReferenceData(nonNullFieldSchema);
            genericField.setAccessible(true);
            genericField.set(referenceData, fieldData);
        } catch (Exception e) {
            LOGGER.warn("Failed to instantiate: genericField={} schema={}", genericField, nonNullFieldSchema, e);
        }
    }

    private static Schema generateReaderReferenceRecordSchema(
            Object referenceData, Schema writerSchema, Function<Type, Schema> typeSchemaLoader) {
        Class<?> referenceDataClass = referenceData.getClass();
        Map<String, Field> fieldsByName = FieldResolution.getAllFieldsByName(referenceDataClass);
        List<Schema.Field> schemaFields = writerSchema.getFields().stream()
                .filter(writerField -> fieldsByName.containsKey(writerField.name()))
                .map(it -> generateReaderReferenceSchemaField(
                        referenceData, fieldsByName.get(it.name()), it, typeSchemaLoader))
                .collect(Collectors.toList());
        return Schema.createRecord(
                referenceDataClass.getCanonicalName(),
                writerSchema.getDoc(),
                null,
                writerSchema.isError(),
                schemaFields);
    }

    private static Schema.Field generateReaderReferenceSchemaField(
            Object referenceData, Field dataField, Schema.Field writerField, Function<Type, Schema> typeSchemaLoader) {
        Object fieldReferenceData = ValueResolution.getFieldValue(referenceData, dataField);
        Schema fieldReaderSchema = fieldReferenceData == null
                ? typeSchemaLoader.apply(dataField.getGenericType())
                : generateFieldReaderReferenceSchema(fieldReferenceData, writerField.schema(), typeSchemaLoader);
        Schema nullSafeFieldReaderSchema = AvroSchemas.isNullable(writerField.schema())
                ? AvroSchemas.makeNullable(fieldReaderSchema)
                : fieldReaderSchema;
        return new Schema.Field(
                writerField.name(),
                nullSafeFieldReaderSchema,
                writerField.doc(),
                writerField.defaultVal(),
                writerField.order());
    }

    private static Schema generateFieldReaderReferenceSchema(
            Object fieldReferenceData, Schema fieldWriterSchema, Function<Type, Schema> typeSchemaLoader) {
        return AvroSchemas.reduceNonNull(fieldWriterSchema)
                .map(nonNullSchema ->
                        generateReaderReferenceSchema(fieldReferenceData, nonNullSchema, typeSchemaLoader))
                .orElseGet(() -> typeSchemaLoader.apply(fieldReferenceData.getClass()));
    }
}
