package net.staticstudios.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ForeignColumn {
    String name();

    String table() default "";

    String schema() default "";

    boolean nullable() default false;

    boolean index() default false;

    String defaultValue() default "";

    String link();

    InsertStrategy insertStrategy() default InsertStrategy.OVERWRITE_EXISTING;

    DeleteStrategy deleteStrategy() default DeleteStrategy.NO_ACTION;
}
