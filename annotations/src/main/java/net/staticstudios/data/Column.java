package net.staticstudios.data;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String name();

    String schema() default "";

    String table() default "";

    boolean index() default false;

    boolean nullable() default false;

    boolean unique() default false;

    String defaultValue() default "";
}
