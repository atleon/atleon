package io.atleon.aws.sns;

/**
 * Represents a descriptor for the destination of an {@link SnsMessage}. While it is typical to
 * send Messages to SNS topics and have consumers subscribe to that topic, SNS also supports
 * sending individual Messages to specific ARNs and phone numbers.
 */
public final class SnsAddress {

    public enum Type {
        TOPIC_ARN,
        TARGET_ARN,
        PHONE_NUMBER
    }

    private final Type type;

    private final String value;

    private SnsAddress(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public static SnsAddress topicArn(String topicArn) {
        return new SnsAddress(Type.TOPIC_ARN, topicArn);
    }

    public static SnsAddress targetArn(String targetArn) {
        return new SnsAddress(Type.TARGET_ARN, targetArn);
    }

    public static SnsAddress phoneNumber(String phoneNumber) {
        return new SnsAddress(Type.PHONE_NUMBER, phoneNumber);
    }

    public Type type() {
        return type;
    }

    public String value() {
        return value;
    }
}
