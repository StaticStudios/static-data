package net.staticstudios.data.v2;

import com.impossibl.postgres.utils.guava.Preconditions;

import java.util.Arrays;

public class PrimaryKey {
    private final String[] columns;
    private final Object[] values;

    public PrimaryKey(String[] columns, Object[] values) {
        Preconditions.checkArgument(columns.length == values.length, "Columns and values must have the same length");
        this.columns = columns;
        this.values = values;
    }

    public static PrimaryKey of(String column, Object value) {
        return new PrimaryKey(new String[]{column}, new Object[]{value});
    }

    public static PrimaryKey of(String column1, Object value1, String column2, Object value2) {
        return new PrimaryKey(new String[]{column1, column2}, new Object[]{value1, value2});
    }

    public static PrimaryKey of(String column1, Object value1, String column2, Object value2, String column3, Object value3) {
        return new PrimaryKey(new String[]{column1, column2, column3}, new Object[]{value1, value2, value3});
    }

    public static PrimaryKey of(String column1, Object value1, String column2, Object value2, String column3, Object value3, String column4, Object value4) {
        return new PrimaryKey(new String[]{column1, column2, column3, column4}, new Object[]{value1, value2, value3, value4});
    }

    public static PrimaryKey of(String column1, Object value1, String column2, Object value2, String column3, Object value3, String column4, Object value4, String column5, Object value5) {
        return new PrimaryKey(new String[]{column1, column2, column3, column4, column5}, new Object[]{value1, value2, value3, value4, value5});
    }

    public Object[] getValues() {
        return values;
    }

    public String[] getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PrimaryKey primaryKey = (PrimaryKey) obj;
        return Arrays.equals(columns, primaryKey.columns) && Arrays.equals(values, primaryKey.values);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(columns);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PrimaryKey{");
        for (int i = 0; i < columns.length; i++) {
            sb.append(columns[i]).append("=").append(values[i]);
            if (i < columns.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");

        return sb.toString();
    }
}
