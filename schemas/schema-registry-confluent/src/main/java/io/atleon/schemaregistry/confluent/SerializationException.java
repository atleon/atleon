package io.atleon.schemaregistry.confluent;

public class SerializationException extends RuntimeException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /* avoid the expensive and useless stack trace for serialization exceptions */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
