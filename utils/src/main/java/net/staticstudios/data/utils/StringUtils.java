package net.staticstudios.data.utils;

import java.util.List;

public class StringUtils {
    public static List<String> parseCommaSeperatedList(String input) {
        return List.of(input.split(","));
    }
}
