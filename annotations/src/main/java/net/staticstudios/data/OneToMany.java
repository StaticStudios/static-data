package net.staticstudios.data;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface OneToMany {
    /**
     * How should this relation be linked?
     * Format "localColumn=foreignColumn"
     *
     * @return The link format
     */
    String link();

    /**
     * This has no effect if the referenced data type extends UniqueData.
     *
     * @return The schema name of the foreign table
     */
    String schema() default "";

    /**
     * This has no effect if the referenced data type extends UniqueData.
     *
     * @return The table name of the foreign table
     */
    String table() default "";


    /**
     * This has no effect if the referenced data type extends UniqueData.
     *
     * @return The column name where the data is stored
     */
    String column() default "value";

    /**
     * This has no effect if the referenced data type extends UniqueData.
     * Should the value column be indexed?
     *
     * @return Whether the data column is indexed
     */
    boolean indexed() default false;

    /**
     * This has no effect if the referenced data type extends UniqueData.
     * Can the column be null?
     *
     * @return Whether the data column is nullable
     */
    boolean nullable() default true;

    /**
     * This has no effect if the referenced data type extends UniqueData.
     * Should the value column be unique?
     *
     * @return Whether the data column is unique
     */
    boolean unique() default false;
}
