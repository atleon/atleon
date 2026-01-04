package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AloStreamNamingTest {

    @Test
    public void streamConfigsCanBeNamedAsKebabCaseWithoutConventionalSuffix() {
        assertEquals(
                "point-a-to-point-b",
                AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(PointAToPointBConfig.class));
        assertEquals(
                "point-a-to-point-b-stream",
                AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(PointAToPointBStreamConfig.class));
        assertEquals(
                "this-is-my-crazy-stream",
                AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(ThisIsMyCRAZYStreamConfig.class));
        assertEquals(
                "this-is-my-other-crazy",
                AloStreamNaming.fromConfigInKebabCaseWithoutConventionalSuffix(ThisIsMyOtherCRAZYConfig.class));
    }

    private static final class PointAToPointBConfig implements AloStreamConfig {}

    private static final class PointAToPointBStreamConfig implements AloStreamConfig {}

    private static final class ThisIsMyCRAZYStreamConfig implements AloStreamConfig {}

    private static final class ThisIsMyOtherCRAZYConfig implements AloStreamConfig {}
}
