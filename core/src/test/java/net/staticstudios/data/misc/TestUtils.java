package net.staticstudios.data.misc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TestUtils {
    public static int getResultCount(ResultSet rs) throws SQLException {
        if (rs.getType() == ResultSet.TYPE_FORWARD_ONLY) {
            int count = rs.getRow();
            while (rs.next()) {
                count++;
            }
            return count;
        }

        int currentRow = rs.getRow();
        try {
            rs.last();
            int rowCount = rs.getRow();
            rs.absolute(currentRow);
            return rowCount;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
