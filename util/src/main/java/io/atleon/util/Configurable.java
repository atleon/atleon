package io.atleon.util;

import java.util.Map;

/**
 * A class that can be configured after instantiation. A class that implements this interface is
 * expected to have a constructor with no parameter.
 */
public interface Configurable {

    void configure(Map<String, ?> properties);
}
