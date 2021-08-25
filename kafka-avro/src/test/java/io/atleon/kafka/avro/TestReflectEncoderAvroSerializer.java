package io.atleon.kafka.avro;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;

public final class TestReflectEncoderAvroSerializer<T> extends ReflectEncoderAvroSerializer<T> {

    private final TestSchemaRegistry registry;

    public TestReflectEncoderAvroSerializer(TestSchemaRegistry registry) {
        this.registry = registry;
    }

    @Override
    public int register(String subject, Schema schema) throws IOException, RestClientException {
        return registry.register(subject, schema);
    }

    @Override
    public Schema getById(int id) throws IOException, RestClientException {
        return registry.getByID(id);
    }
}
