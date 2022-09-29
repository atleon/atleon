package io.atleon.core;

import java.util.regex.Pattern;

public final class AloStreamNaming {

    private static final Pattern SUFFIX_PATTERN = Pattern.compile("(Config)$");

    private AloStreamNaming() {

    }

    public static String fromConfigInKebabCaseWithoutConventionalSuffix(Class<? extends AloStreamConfig> configClass) {
        return toKebabCase(SUFFIX_PATTERN.matcher(configClass.getSimpleName()).replaceAll(""));
    }

    private static String toKebabCase(String string) {
        StringBuilder result = new StringBuilder();
        result.append(Character.toLowerCase(string.charAt(0)));
        for (int i = 1; i < string.length(); i++) {
            char character = string.charAt(i);
            if (Character.isUpperCase(character)) {
                result.append("-");
                result.append(Character.toLowerCase(character));
            } else {
                result.append(character);
            }
        }
        return result.toString();
    }
}
