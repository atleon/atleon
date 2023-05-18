package io.atleon.schema;

import java.io.ByteArrayOutputStream;

public interface SchematicPreSerializer<S> {

    S apply(S schema, ByteArrayOutputStream outputStream);
}
