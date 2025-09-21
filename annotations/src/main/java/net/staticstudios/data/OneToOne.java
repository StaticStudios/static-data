package net.staticstudios.data;


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

    UpdateStrategy updateStrategy() default UpdateStrategy.CASCADE;
}
