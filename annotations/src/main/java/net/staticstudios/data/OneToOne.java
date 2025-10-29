package net.staticstudios.data;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface OneToOne {
    /**
     * How should this relation be linked?
     * Format "localColumn=foreignColumn"
     *
     * @return The link format
     */
    String link();

    //todo: option to force not null?

}
