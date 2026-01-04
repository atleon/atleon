package io.atleon.core;

import java.util.regex.Pattern;

public final class AloStreamNaming {

    private static final Pattern SUFFIX_PATTERN = Pattern.compile("(Config)$");

    private AloStreamNaming() {}

    public static String fromConfigInKebabCaseWithoutConventionalSuffix(Class<? extends AloStreamConfig> configClass) {
        return toKebabCase(SUFFIX_PATTERN.matcher(configClass.getSimpleName()).replaceAll(""));
    }

    private static String toKebabCase(String string) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            if (isStartOfAnotherWord(string, i)) {
                result.append("-");
            }
            result.append(Character.toLowerCase(string.charAt(i)));
        }
        return result.toString();
    }

    private static boolean isStartOfAnotherWord(String string, int index) {
        if (index <= 0 || !Character.isUpperCase(string.charAt(index))) {
            return false;
        } else if (index + 1 < string.length() && !Character.isUpperCase(string.charAt(index + 1))) {
            return true;
        } else {
            return !Character.isUpperCase(string.charAt(index - 1));
        }
    }
}
