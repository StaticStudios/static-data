package net.staticstudios.data.util;

import net.staticstudios.data.delete.DeleteStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface OneToOne {
    /**
     * How should this relation be linked?
     * Format "localColumn=foreignColumn"
     *
     * @return The link format
     */
    String link();

    DeleteStrategy deleteStrategy() default DeleteStrategy.NO_ACTION;
}
