package io.atleon.avro;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.atleon.util.TypeResolution;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

public final class AvroSchemas {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private AvroSchemas() {}

    public static Schema removeJavaProperties(Schema schema) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(schema.toString());
            removeProperty(node, "avro.java.string");
            return createParser().parse(node.toString());
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not parse schema: " + schema);
        }
    }

    public static Schema getOrReflectNullable(Object data) {
        return makeNullable(getOrReflect(data));
    }

    public static Schema getOrReflect(Object data) {
        return getOrSupply(data, () -> ReflectData.get().getSchema(data.getClass()));
    }

    public static Schema getOrSupply(Object data, Supplier<Schema> schemaSupplier) {
        try {
            return data instanceof GenericContainer
                    ? GenericContainer.class.cast(data).getSchema()
                    : SpecificData.get().getSchema(TypeResolution.safelyGetClass(data));
        } catch (AvroRuntimeException e) {
            return schemaSupplier.get();
        }
    }

    public static Schema makeNullable(Schema schema) {
        return isNull(schema) ? schema : ReflectData.makeNullable(schema);
    }

    public static Optional<Schema> reduceNonNull(Schema schema) {
        if (isUnion(schema)) {
            return schema.getTypes().stream().reduce((schema1, schema2) -> isNull(schema1) ? schema2 : schema1);
        } else {
            return isNull(schema) ? Optional.empty() : Optional.of(schema);
        }
    }

    public static boolean isNullable(Schema schema) {
        return isUnion(schema) && schema.getTypes().stream().anyMatch(AvroSchemas::isNull);
    }

    public static boolean isUnion(Schema schema) {
        return schema.getType() == Schema.Type.UNION;
    }

    public static boolean isRecord(Schema schema) {
        return schema.getType() == Schema.Type.RECORD;
    }

    public static boolean isNull(Schema schema) {
        return schema.getType() == Schema.Type.NULL;
    }

    private static JsonMapper createObjectMapper() {
        return JsonMapper.builder()
                .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
                .build();
    }

    private static Schema.Parser createParser() {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        return parser;
    }

    private static void removeProperty(JsonNode node, String propertyName) {
        if (node.isObject()) {
            ObjectNode.class.cast(node).remove(propertyName);
        }

        if (node.isObject() || node.isArray()) {
            node.elements().forEachRemaining(element -> removeProperty(element, propertyName));
        }
    }
}
