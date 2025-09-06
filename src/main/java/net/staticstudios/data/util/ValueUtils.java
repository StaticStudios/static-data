package net.staticstudios.data.util;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ValueUtils {
    private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)}");
    @VisibleForTesting
    public static EnvironmentVariableAccessor ENVIRONMENT_VARIABLE_ACCESSOR = new EnvironmentVariableAccessor();

    public static String parseValue(String encoded) {
        Preconditions.checkNotNull(encoded, "Encoded value cannot be null");
        Matcher matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(encoded);
        if (matcher.matches()) {
            String varName = matcher.group(1);
            String value = ENVIRONMENT_VARIABLE_ACCESSOR.getEnv(varName);
            Preconditions.checkArgument(value != null, "Environment variable " + varName + " is not set");
            return value;
        } else {
            return encoded;
        }
    }
}
