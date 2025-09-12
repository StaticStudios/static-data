package net.staticstudios.data.parse;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

//todo: annotations would break compatability, but they make static analysis easier for meta data parsing and for building sql
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String name();

    String schema() default "";

    String table() default "";

    boolean index() default false; //todo: this

    boolean nullable() default false; //todo: this
}
