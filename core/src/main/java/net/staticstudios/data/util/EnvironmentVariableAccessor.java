package net.staticstudios.data.util;

import java.util.HashMap;
import java.util.Map;

public class EnvironmentVariableAccessor {
    private final Map<String, String> override = new HashMap<>();

    public void set(String key, String value) {
        override.put(key, value);
    }

    public String getEnv(String name) {
        String value = override.get(name);
        if (value != null) {
            return value;
        }
        return System.getenv(name);
    }
}
