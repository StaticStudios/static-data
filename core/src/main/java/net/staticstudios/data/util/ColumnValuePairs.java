package net.staticstudios.data.util;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;

public final class ColumnValuePairs implements Iterable<ColumnValuePair> {
    public static final ColumnValuePairs EMPTY = new ColumnValuePairs();

    private final ColumnValuePair[] pairs;

    public ColumnValuePairs(ColumnValuePair... pairs) {
        this.pairs = pairs.clone();
        Arrays.sort(this.pairs, Comparator.comparing(ColumnValuePair::column));
    }

    public ColumnValuePair[] getPairs() {
        return pairs;
    }

    public Stream<ColumnValuePair> stream() {
        return Arrays.stream(pairs);
    }

    public boolean isEmpty() {
        return pairs.length == 0;
    }

    @Override
    public java.util.@NotNull Iterator<ColumnValuePair> iterator() {
        return new java.util.Iterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < pairs.length;
            }

            @Override
            public ColumnValuePair next() {
                return pairs[index++];
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ColumnValuePairs other)) return false;
        if (this.pairs.length != other.pairs.length) return false;
        for (int i = 0; i < this.pairs.length; i++) {
            if (!this.pairs[i].equals(other.pairs[i])) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(pairs);
    }

    @Override
    public String toString() {
        return Arrays.toString(pairs);
    }
}
