package io.atleon.core;

public interface AloStreamConfig {

    default String name() {
        return AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(getClass());
    }
}
