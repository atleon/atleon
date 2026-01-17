package io.atleon.schemaregistry.confluent;

import io.confluent.kafka.schemaregistry.ParsedSchema;

final class ParsedSchemas {

    private ParsedSchemas() {}

    public static <S> S extractTypedRawSchema(ParsedSchema parsedSchema, Class<? extends S> type) {
        Object rawSchema = parsedSchema.rawSchema();
        if (type.isInstance(rawSchema)) {
            return type.cast(rawSchema);
        } else {
            throw new IllegalArgumentException(
                    "Expected schema to be of type=" + type + " but got rawSchema=" + rawSchema);
        }
    }
}
