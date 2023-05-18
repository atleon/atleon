package io.atleon.schemaregistry.confluent;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;

public abstract class RegistrySerDe extends AbstractKafkaSchemaSerDe {

    public void configure(RegistrySerDeConfig config) {
        configureClientProperties(config, createSchemaProvider());
    }

    protected abstract SchemaProvider createSchemaProvider();
}
