package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.primative.Primitive;
import net.staticstudios.data.primative.Primitives;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNull;

public class PrimitivesTest extends DataTest {
    private Connection postgresConnection;
    private Connection h2Connection;

    @BeforeEach
    public void setup() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        postgresConnection = getConnection();
        h2Connection = getH2Connection(dataManager);
    }

    @Test
    public void testString() throws Exception {
        test(Primitives.STRING, "Hello, World!");
    }

    @Test
    public void testInteger() throws Exception {
        test(Primitives.INTEGER, 12345);
    }

    @Test
    public void testLong() throws Exception {
        test(Primitives.LONG, 123456789L);
    }

    @Test
    public void testFloat() throws Exception {
        test(Primitives.FLOAT, 123.45f);
    }

    @Test
    public void testDouble() throws Exception {
        test(Primitives.DOUBLE, 123456.789);
    }

    @Test
    public void testBoolean() throws Exception {
        test(Primitives.BOOLEAN, true);
        test(Primitives.BOOLEAN, false);
    }

    @Test
    public void testUUID() throws Exception {
        test(Primitives.UUID, UUID.randomUUID());
    }

    @Test
    public void testTimestamp() throws Exception {
        test(Primitives.TIMESTAMP, new Timestamp(System.currentTimeMillis()));
    }

//    @Test
//    public void testByteArray() throws Exception {
//        test(Primitives.BYTE_ARRAY, new byte[]{1, 2, 3, 4, 5});
//    }

    private <T> void test(Primitive<T> primitive, T value) throws Exception {
        // note that encoding and decoding is only in the context of PG. H2 byte[] is in a different format, but we don't every encode/decode when working with H2.
        @Language("SQL") String sql = "CREATE TABLE test (id INT PRIMARY KEY, val %s)";
        T decoded = primitive.decode(primitive.encode(value));
        assertEquals(value, decoded);
        assertNull(primitive.decode(null));
        assertNull(primitive.encode(null));

        try (Statement h2Statement = h2Connection.createStatement()) {
            h2Statement.execute("DROP TABLE IF EXISTS test");
            h2Statement.execute(String.format(sql, primitive.getH2SQLType()));
        }

        try (PreparedStatement h2Statement = h2Connection.prepareStatement("INSERT INTO test (id, val) VALUES (?, ?)")) {
            h2Statement.setInt(1, 1);
            h2Statement.setObject(2, value);
            h2Statement.executeUpdate();
        }

        try (Statement h2Statement = h2Connection.createStatement()) {
            try (ResultSet rs = h2Statement.executeQuery("SELECT val FROM test WHERE id = 1")) {
                if (rs.next()) {
                    Object fromDb = rs.getObject("val", primitive.getRuntimeType());
                    assertEquals(value, fromDb);
                }
            }
        }

        try (Statement postgresStatement = postgresConnection.createStatement()) {
            postgresStatement.execute("DROP TABLE IF EXISTS test");
            postgresStatement.execute(String.format(sql, primitive.getPgSQLType()));
            postgresStatement.execute(String.format("INSERT INTO test (id, val) VALUES (1, '%s'::%s)", primitive.encode(value), primitive.getPgSQLType()));
            try (ResultSet rs = postgresStatement.executeQuery("SELECT val FROM test WHERE id = 1")) {
                if (rs.next()) {
                    Object fromDb = rs.getObject("val");
                    assertEquals(value, fromDb);
                }
            }
        }
    }

    public void assertEquals(Object expected, Object actual) {
        if (expected instanceof byte[] expectedBytes && actual instanceof byte[] actualBytes) {
            Assertions.assertArrayEquals(expectedBytes, actualBytes);
        } else {
            Assertions.assertEquals(expected, actual);
        }
    }

}