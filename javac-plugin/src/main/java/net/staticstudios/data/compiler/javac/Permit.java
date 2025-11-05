package net.staticstudios.data.compiler.javac;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Permit {
    public static Field getField(Class<?> c, String fName) throws NoSuchFieldException {
        Field f = null;
        Class<?> d = c;
        while (d != null) {
            try {
                f = d.getDeclaredField(fName);
                break;
            } catch (NoSuchFieldException e) {
            }
            d = d.getSuperclass();
        }
        if (f == null) throw new NoSuchFieldException(c.getName() + " :: " + fName);

        return setAccessible(f);
    }

    public static <T extends AccessibleObject> T setAccessible(T accessor) {
        accessor.setAccessible(true);
        return accessor;
    }

    public static Method getMethod(Class<?> c, String mName, Class<?>... parameterTypes) throws NoSuchMethodException {
        Method m = null;
        Class<?> oc = c;
        while (c != null) {
            try {
                m = c.getDeclaredMethod(mName, parameterTypes);
                break;
            } catch (NoSuchMethodException e) {
            }
            c = c.getSuperclass();
        }

        if (m == null) throw new NoSuchMethodException(oc.getName() + " :: " + mName + "(args)");
        return setAccessible(m);
    }
}
