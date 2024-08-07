package io.atleon.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.util.ClassUtils;

import java.util.Optional;
import java.util.function.Function;

final class Contexts {

    private Contexts() {

    }

    public static boolean isPropertySetToFalse(ApplicationContext context, String property) {
        return "false".equalsIgnoreCase(context.getEnvironment().getProperty(property));
    }

    public static boolean isPropertySetToTrue(ConditionContext context, String property) {
        return "true".equalsIgnoreCase(context.getEnvironment().getProperty(property));
    }

    public static boolean isPropertySetToFalse(ConditionContext context, String property) {
        return "false".equalsIgnoreCase(context.getEnvironment().getProperty(property));
    }

    public static Optional<String> parseValue(ConfigurableApplicationContext context, String expression) {
        return Optional.ofNullable(context.getBeanFactory().resolveEmbeddedValue(expression));
    }

    public static boolean isClassPresent(ConditionContext context, String className) {
        ClassLoader classLoader = deduceClassLoader(context);
        try {
            Class.forName(className, false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static ClassLoader deduceClassLoader(ConditionContext context) {
        if (context.getClassLoader() != null) {
            return context.getClassLoader();
        } else {
            ClassLoader defaultClassLoader = ClassUtils.getDefaultClassLoader();
            return defaultClassLoader != null ? defaultClassLoader : Contexts.class.getClassLoader();
        }
    }
}
