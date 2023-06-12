package io.atleon.aws.sqs;

public class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public Long deserialize(String data) {
        return Long.valueOf(data);
    }
}
