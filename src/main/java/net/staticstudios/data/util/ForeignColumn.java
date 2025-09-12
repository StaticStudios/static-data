package net.staticstudios.data.util;

import net.staticstudios.data.delete.DeleteStrategy;
import net.staticstudios.data.insert.InsertStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ForeignColumn {
    String name();

    String table() default "";

    String schema() default "";

    boolean nullable() default false;

    boolean index() default false;

    String link();

    InsertStrategy insertStrategy() default InsertStrategy.OVERWRITE_EXISTING;

    DeleteStrategy deleteStrategy() default DeleteStrategy.NO_ACTION;
}
