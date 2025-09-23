package net.staticstudios.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is only applicable to {@link net.staticstudios.data.PersistentValue}s.
 * Instead of propagating updates to the real database instantly, only the latest update will be made every Nms.
 * This is useful for values that update very frequently, since this could otherwise overwhelm the task queue.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface UpdateInterval {
    /**
     * How long should we wait between updates, in milliseconds?
     *
     * @return The interval in milliseconds
     */
    int value();
}
