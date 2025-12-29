package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.account.*;
import net.staticstudios.data.mock.wrapper.booleanprimitive.BooleanWrapper;
import net.staticstudios.data.mock.wrapper.booleanprimitive.BooleanWrapperDataClass;
import net.staticstudios.data.mock.wrapper.booleanprimitive.BooleanWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.bytearrayprimitive.ByteArrayWrapper;
import net.staticstudios.data.mock.wrapper.bytearrayprimitive.ByteArrayWrapperDataClass;
import net.staticstudios.data.mock.wrapper.bytearrayprimitive.ByteArrayWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.doubleprimitive.DoubleWrapper;
import net.staticstudios.data.mock.wrapper.doubleprimitive.DoubleWrapperDataClass;
import net.staticstudios.data.mock.wrapper.doubleprimitive.DoubleWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.floatprimitive.FloatWrapper;
import net.staticstudios.data.mock.wrapper.floatprimitive.FloatWrapperDataClass;
import net.staticstudios.data.mock.wrapper.floatprimitive.FloatWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.integerprimitive.IntegerWrapper;
import net.staticstudios.data.mock.wrapper.integerprimitive.IntegerWrapperDataClass;
import net.staticstudios.data.mock.wrapper.integerprimitive.IntegerWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.longprimitive.LongWrapper;
import net.staticstudios.data.mock.wrapper.longprimitive.LongWrapperDataClass;
import net.staticstudios.data.mock.wrapper.longprimitive.LongWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.stringprimitive.StringWrapper;
import net.staticstudios.data.mock.wrapper.stringprimitive.StringWrapperDataClass;
import net.staticstudios.data.mock.wrapper.stringprimitive.StringWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.timestampprimitive.TimestampWrapper;
import net.staticstudios.data.mock.wrapper.timestampprimitive.TimestampWrapperDataClass;
import net.staticstudios.data.mock.wrapper.timestampprimitive.TimestampWrapperValueSerializer;
import net.staticstudios.data.mock.wrapper.uuidprimitive.UUIDWrapper;
import net.staticstudios.data.mock.wrapper.uuidprimitive.UUIDWrapperDataClass;
import net.staticstudios.data.mock.wrapper.uuidprimitive.UUIDWrapperValueSerializer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class CustomTypeTest extends DataTest {
    @Test
    public void testCustomTypesSetGet() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new AccountDetailsValueSerializer());
        dataManager.registerValueSerializer(new AccountSettingsValueSerializer());
        dataManager.load(MockAccount.class);
        dataManager.finishLoading();

        MockAccount account = MockAccount.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        assertNull(account.settings.get());
        assertNull(account.details.get());

        AccountDetails details = new AccountDetails("detail1", "detail2");
        AccountSettings settings = new AccountSettings(true, false);
        account.settings.set(settings);
        account.details.set(details);

        assertEquals(settings, account.settings.get());
        assertEquals(details, account.details.get());
    }

    @Test
    public void testCustomTypesLoad() throws SQLException {
        AccountDetailsValueSerializer detailsSerializer = new AccountDetailsValueSerializer();
        AccountSettingsValueSerializer settingsSerializer = new AccountSettingsValueSerializer();
        AccountDetails details = new AccountDetails("detail1", "detail2");
        AccountSettings settings = new AccountSettings(true, false);

        String detailsSerialized = detailsSerializer.serialize(details);
        String settingsSerialized = settingsSerializer.serialize(settings);

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE \"public\".\"accounts\" (\"id\" INTEGER PRIMARY KEY, \"settings\" TEXT NULL);");
            statement.execute("CREATE TABLE \"public\".\"account_details\" (\"account_id\" INTEGER PRIMARY KEY, \"details\" TEXT NULL, FOREIGN KEY (\"account_id\") REFERENCES \"accounts\"(\"id\") ON DELETE CASCADE);");

            statement.execute("INSERT INTO \"public\".\"accounts\" (\"id\", \"settings\") VALUES (1, '" + settingsSerialized + "');");
            statement.execute("INSERT INTO \"public\".\"account_details\" (\"account_id\", \"details\") VALUES (1, '" + detailsSerialized + "');");
        }

        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(detailsSerializer);
        dataManager.registerValueSerializer(settingsSerializer);
        dataManager.load(MockAccount.class);
        dataManager.finishLoading();

        MockAccount account = MockAccount.query(dataManager).where(w -> w.idIs(1)).findOne();
        assertNotNull(account);
        assertEquals(settings, account.settings.get());
        assertEquals(details, account.details.get());
    }

    @Test
    public void testCustomTypeWithStringPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new StringWrapperValueSerializer());
        dataManager.load(StringWrapperDataClass.class);
        StringWrapperDataClass data = StringWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new StringWrapper("Hello, World!"))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals("Hello, World!", data.value.get().value());
        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals("Hello, World!", rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithIntegerPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new IntegerWrapperValueSerializer());
        dataManager.load(IntegerWrapperDataClass.class);
        IntegerWrapperDataClass data = IntegerWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new IntegerWrapper(42))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(42, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_integer\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_integer\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithLongPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new LongWrapperValueSerializer());
        dataManager.load(LongWrapperDataClass.class);
        LongWrapperDataClass data = LongWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new LongWrapper(1234567890123L))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(1234567890123L, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_long\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(1234567890123L, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_long\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithFloatPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new FloatWrapperValueSerializer());
        dataManager.load(FloatWrapperDataClass.class);
        FloatWrapperDataClass data = FloatWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new FloatWrapper(3.14f))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(3.14f, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_float\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(3.14f, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_float\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithDoublePrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new DoubleWrapperValueSerializer());
        dataManager.load(DoubleWrapperDataClass.class);
        DoubleWrapperDataClass data = DoubleWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new DoubleWrapper(2.71828))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(2.71828, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_double\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(2.71828, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_double\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithBooleanPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new BooleanWrapperValueSerializer());
        dataManager.load(BooleanWrapperDataClass.class);
        BooleanWrapperDataClass data = BooleanWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new BooleanWrapper(true))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertTrue(data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_boolean\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(true, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_boolean\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithUUIDPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new UUIDWrapperValueSerializer());
        dataManager.load(UUIDWrapperDataClass.class);
        java.util.UUID uuid = java.util.UUID.randomUUID();
        UUIDWrapperDataClass data = UUIDWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new UUIDWrapper(uuid))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(uuid, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_uuid\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(uuid, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_uuid\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithTimestampPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new TimestampWrapperValueSerializer());
        dataManager.load(TimestampWrapperDataClass.class);
        java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
        TimestampWrapperDataClass data = TimestampWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new TimestampWrapper(timestamp))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertEquals(timestamp, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_timestamp\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertEquals(timestamp, rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_timestamp\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    @Test
    public void testCustomTypeWithByteArrayPrimitive() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new ByteArrayWrapperValueSerializer());
        dataManager.load(ByteArrayWrapperDataClass.class);
        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        ByteArrayWrapperDataClass data = ByteArrayWrapperDataClass.builder(dataManager)
                .id(1)
                .value(new ByteArrayWrapper(bytes))
                .insert(InsertMode.SYNC);

        assertNotNull(data.value.get());
        assertArrayEquals(bytes, data.value.get().value());

        waitForDataPropagation();

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_bytea\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertArrayEquals(bytes, (byte[]) rs.getObject("val"));
            }
        }

        data.value.set(null);
        assertNull(data.value.get());
        waitForDataPropagation();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT * FROM \"public\".\"custom_type_test_bytea\" WHERE \"id\" = 1;")) {
                assertTrue(rs.next());
                assertNull(rs.getObject("val"));
            }
        }
    }

    //todo: test postgres listen/notify with custom types. specifically ensure the encode and decode functions are correct
}