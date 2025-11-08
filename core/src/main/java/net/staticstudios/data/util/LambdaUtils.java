package net.staticstudios.data.util;

import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.List;

public class LambdaUtils {

    public static void assertLambdaDoesntCapture(Object lambda, @Nullable List<Class<?>> disallowedTypes, @Nullable String hint) {
        Class<?> lambdaClass = lambda.getClass();
        String message;
        boolean allowed = true;
        if (disallowedTypes == null) {
            allowed = lambdaClass.getDeclaredFields().length == 0;
            message = "Lambda expression captures variables from its enclosing scope. It must act as a static function. Did you reference 'this' or a member variable?";
        } else {
            message = "Lambda expression captures disallowed variable types from its enclosing scope. Type that was captured: ";
            for (Class<?> disallowedType : disallowedTypes) {
                for (Field field : lambdaClass.getDeclaredFields()) {
                    if (disallowedType.isAssignableFrom(field.getType())) {
                        allowed = false;
                        message += field.getType().getSimpleName();
                        break;
                    }
                }
                if (!allowed) {
                    break;
                }
            }
        }
        if (!allowed) {
            if (hint != null && !hint.isEmpty()) {
                message += " Hint: " + hint;
            }
            throw new IllegalArgumentException(message);
        }
    }

    public static void assertLambdaDoesntCapture(Object lambda, @Nullable String hint) {
        assertLambdaDoesntCapture(lambda, null, hint);
    }
}
