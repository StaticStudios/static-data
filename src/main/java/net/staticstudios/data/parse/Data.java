package net.staticstudios.data.parse;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

//todo: annotations would break compatability, but they make static analysis easier for meta data parsing and for building sql
@Retention(RetentionPolicy.RUNTIME) //todo: we should support env variables in here as well.
public @interface Data {
    String schema();

    String table();
}
