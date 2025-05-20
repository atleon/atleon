package io.atleon.protobuf;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

public final class ProtobufKafkaSerializer<T extends Message> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        return data == null ? null : data.toByteArray();
    }
}
