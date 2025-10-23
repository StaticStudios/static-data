package net.staticstudios.data;

import net.staticstudios.data.util.EnvironmentVariableAccessor;
import net.staticstudios.data.util.ValueUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ValueParseTest {

    @BeforeAll
    public static void setup() {
        ValueUtils.ENVIRONMENT_VARIABLE_ACCESSOR = new EnvironmentVariableAccessor() {
            @Override
            public String getEnv(String name) {
                return switch (name) {
                    case "env_var" -> "value_here";
                    case "ENV_VAR2" -> "VALUE2_HERE";
                    default -> null;
                };
            }
        };
    }

    @Test
    public void testParse() {
        assertEquals("value", ValueUtils.parseValue("value"));
        assertEquals("value_here", ValueUtils.parseValue("${env_var}"));
        assertEquals("VALUE2_HERE", ValueUtils.parseValue("${ENV_VAR2}"));
    }
}