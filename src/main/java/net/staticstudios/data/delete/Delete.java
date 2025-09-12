package net.staticstudios.data.delete;

import net.staticstudios.data.DeleteStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Delete {
    DeleteStrategy value() default DeleteStrategy.NO_ACTION;
}
