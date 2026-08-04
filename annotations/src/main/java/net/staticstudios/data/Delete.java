package net.staticstudios.data;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configures how data represented by a relationship field is handled when the field's holder is deleted.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Delete {
    /**
     * Controls what happens to data represented by the annotated field when its holder is deleted.
     *
     * @return the deletion strategy for the relationship
     */
    DeleteStrategy value();
}
