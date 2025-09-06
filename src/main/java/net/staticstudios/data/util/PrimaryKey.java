package net.staticstudios.data.util;

import java.util.List;

public interface PrimaryKey {

    List<ColumnValuePair> getWhereClause();

    record ColumnValuePair(String column, Object value) {
    }
}
