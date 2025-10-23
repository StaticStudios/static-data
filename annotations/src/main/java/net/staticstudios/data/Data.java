package net.staticstudios.data;
//todo: the annotations package can be removed and everything can be put back into core once the processor is gone

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Data {
    String schema();

    String table();
}
