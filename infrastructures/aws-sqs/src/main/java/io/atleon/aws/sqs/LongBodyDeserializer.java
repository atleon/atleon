package io.atleon.aws.sqs;

import io.atleon.util.Parsing;

public final class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public Long deserialize(String data) {
        return Parsing.toLong(data);
    }
}
