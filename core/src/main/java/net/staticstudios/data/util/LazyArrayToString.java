package net.staticstudios.data.util;

import java.util.Arrays;

public class LazyArrayToString {
    private final Object[] arr;

    public LazyArrayToString(Object[] arr) {
        this.arr = arr;
    }

    public static LazyArrayToString of(Object[] arr) {
        return new LazyArrayToString(arr);
    }

    @Override
    public String toString() {
        return Arrays.toString(arr);
    }
}
