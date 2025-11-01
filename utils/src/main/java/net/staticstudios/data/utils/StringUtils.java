package net.staticstudios.data.utils;

import java.util.List;

public class StringUtils {
    public static List<String> parseCommaSeperatedList(String input) {
        return List.of(input.split(","));
    }

    public static String capitalize(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        return input.substring(0, 1).toUpperCase() + input.substring(1);
    }
}
