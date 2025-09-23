package net.staticstudios.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Column {
    String name();

    String schema() default "";

    String table() default "";

    boolean index() default false;

    boolean nullable() default false;

    boolean unique() default false;

    String defaultValue() default "";
}
