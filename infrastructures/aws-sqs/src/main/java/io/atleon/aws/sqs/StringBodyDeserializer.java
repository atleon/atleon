package io.atleon.aws.sqs;

/**
 * An SQS {@link BodyDeserializer} that simply returns SQS native Message String bodies.
 */
public final class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public String deserialize(String data) {
        return data;
    }
}
