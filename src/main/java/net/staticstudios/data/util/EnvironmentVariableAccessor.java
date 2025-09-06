package net.staticstudios.data.util;

public class EnvironmentVariableAccessor {
    public String getEnv(String name) {
        return System.getenv(name);
    }
}
