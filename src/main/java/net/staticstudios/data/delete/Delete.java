package net.staticstudios.data.delete;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Delete {
    DeleteStrategy value() default DeleteStrategy.NO_ACTION;
}
