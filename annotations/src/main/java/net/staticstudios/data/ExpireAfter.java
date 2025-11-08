package net.staticstudios.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used for CachedValues.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ExpireAfter {
    /**
     * How long until the value in redis is deleted?
     * In other words, how long until we revert back to the fallback value?
     *
     * @return The duration in seconds.
     */
    int value() default -1;
}
