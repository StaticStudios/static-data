package net.staticstudios.data.insert;

import net.staticstudios.data.InsertStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Insert {
    InsertStrategy value() default InsertStrategy.OVERWRITE_EXISTING;
}
