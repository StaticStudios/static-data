package net.staticstudios.data;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@ImplicitWrite
public @interface OneToOne {
    /**
     * How should this relation be linked?
     * Format "localColumn=foreignColumn"
     *
     * @return The link format
     */
    String link();

    //todo: option to force not null?

    /**
     * Should a foreign key constraint be created for this relation?
     *
     * @return Whether to create a foreign key constraint
     */
    boolean fkey() default true;

    /**
     * When making an update to this relaction via #set(T), should the columns in the referenced table be updated instead of the local columns?
     *
     * @return Whether to update the referenced columns instead of the local columns when updating this relation
     */
    boolean updateReferencedColumns() default false;

}
