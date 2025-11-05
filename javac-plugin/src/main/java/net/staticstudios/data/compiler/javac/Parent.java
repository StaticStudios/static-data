package net.staticstudios.data.compiler.javac;

import java.io.OutputStream;

@SuppressWarnings("all")
public class Parent {
    static final Object staticObj = OutputStream.class;
    private static volatile boolean staticSecond;
    private static volatile boolean staticThird;
    boolean first;
    volatile Object second;
}