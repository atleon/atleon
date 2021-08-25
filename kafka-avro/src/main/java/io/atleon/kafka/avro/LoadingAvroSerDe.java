package io.atleon.kafka.avro;

import org.apache.avro.Schema;

import java.lang.reflect.Type;

public abstract class LoadingAvroSerDe extends AvroSerDe {

    protected abstract Schema loadTypeSchema(Type dataType);
}
