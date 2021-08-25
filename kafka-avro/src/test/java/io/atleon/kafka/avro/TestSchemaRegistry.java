package io.atleon.kafka.avro;

import org.apache.avro.Schema;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.UnaryOperator;

public final class TestSchemaRegistry {

    private static final NavigableMap<Integer, Schema> SCHEMAS_BY_ID = new ConcurrentSkipListMap<>();

    private UnaryOperator<Schema> registrationHook = UnaryOperator.identity();

    public int register(String subject, Schema schema) {
        synchronized (SCHEMAS_BY_ID) {
            int id = SCHEMAS_BY_ID.isEmpty() ? 1 : SCHEMAS_BY_ID.lastKey() + 1;
            SCHEMAS_BY_ID.put(id, registrationHook.apply(schema));
            return id;
        }
    }

    public Schema getByID(int id) {
        return SCHEMAS_BY_ID.get(id);
    }

    public void setRegistrationHook(UnaryOperator<Schema> registryHook) {
        this.registrationHook = registryHook;
    }
}
