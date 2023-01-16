package io.atleon.aws.sqs;

import java.util.Map;

/**
 * An SQS {@link BodyDeserializer} that simply returns SQS native Message String bodies.
 */
public final class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public String deserialize(String body) {
        return body;
    }
}
