package io.atleon.kafka.avro;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.Schema;

public final class TestReflectDecoderAvroDeserializer<T> extends ReflectDecoderAvroDeserializer<T> {

    private final TestSchemaRegistry registry;

    public TestReflectDecoderAvroDeserializer(TestSchemaRegistry registry) {
        this.registry = registry;
    }

    @Override
    public int register(String subject, ParsedSchema schema) {
        return registry.register(subject, AvroSchema.class.cast(schema).rawSchema());
    }

    @Override
    public ParsedSchema getSchemaById(int id) {
        Schema schema = registry.getByID(id);
        return schema == null ? null : new AvroSchema(schema);
    }
}
