package io.atleon.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AloStreamNamingTest {

    @Test
    public void streamConfigsCanBeNamedAsKebabCaseWithoutConventionalSuffix() {
        assertEquals(
            "point-a-to-point-b",
            AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(PointAToPointBConfig.class)
        );
        assertEquals(
            "point-a-to-point-b-stream",
            AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(PointAToPointBStreamConfig.class)
        );
    }

    private static final class PointAToPointBConfig implements AloStreamConfig {

    }

    private static final class PointAToPointBStreamConfig implements AloStreamConfig {

    }
}