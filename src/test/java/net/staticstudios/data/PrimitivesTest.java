package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.MockEnvironment;
import net.staticstudios.data.mock.primative.*;
import net.staticstudios.data.primative.Primitives;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.RetryingTest;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class PrimitivesTest extends DataTest {

    @BeforeEach
    public void init() {
        try (Statement statement = getConnection().createStatement()) {
            statement.executeUpdate("""
                    drop schema if exists primitive cascade;
                    create schema if not exists primitive;
                    create table if not exists primitive.string_test (
                        id uuid primary key,
                        value text
                    );
                    
                    create table if not exists primitive.character_test (
                        id uuid primary key,
                        value char(1) not null
                    );
                    
                    create table if not exists primitive.byte_test (
                        id uuid primary key,
                        value smallint not null
                    );
                    
                    create table if not exists primitive.short_test (
                        id uuid primary key,
                        value smallint not null
                    );
                    
                    create table if not exists primitive.integer_test (
                        id uuid primary key,
                        value integer not null
                    );
                    
                    create table if not exists primitive.long_test (
                        id uuid primary key,
                        value bigint not null
                    );
                    
                    create table if not exists primitive.float_test (
                        id uuid primary key,
                        value real not null
                    );
                    
                    create table if not exists primitive.double_test (
                        id uuid primary key,
                        value double precision not null
                    );
                    
                    create table if not exists primitive.boolean_test (
                        id uuid primary key,
                        value boolean not null
                    );
                    
                    create table if not exists primitive.uuid_test (
                        id uuid primary key,
                        value uuid
                    );
                    
                    create table if not exists primitive.timestamp_test (
                        id uuid primary key,
                        value timestamp
                    );
                    
                    create table if not exists primitive.byte_array_test (
                        id uuid primary key,
                        value bytea
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getMockEnvironments().forEach(env -> {
            DataManager dataManager = env.dataManager();
            dataManager.loadAll(StringPrimitiveTestObject.class);
            dataManager.loadAll(CharacterPrimitiveTestObject.class);
            dataManager.loadAll(BytePrimitiveTestObject.class);
            dataManager.loadAll(ShortPrimitiveTestObject.class);
            dataManager.loadAll(IntegerPrimitiveTestObject.class);
            dataManager.loadAll(LongPrimitiveTestObject.class);
            dataManager.loadAll(FloatPrimitiveTestObject.class);
            dataManager.loadAll(DoublePrimitiveTestObject.class);
            dataManager.loadAll(BooleanPrimitiveTestObject.class);
            dataManager.loadAll(UUIDPrimitiveTestObject.class);
            dataManager.loadAll(TimestampPrimitiveTestObject.class);
            dataManager.loadAll(ByteArrayPrimitiveTestObject.class);
        });
    }

    //-----Test nullability START-----

    @RetryingTest(5)
    public void testStringPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are allowed in strings, so this should be fine.
        StringPrimitiveTestObject obj = StringPrimitiveTestObject.createSync(dataManager, "Hello, World!");
        StringPrimitiveTestObject.createSync(dataManager, null);

        obj.setValue(null); //This should not throw an NPE
    }

    @RetryingTest(5)
    public void testCharacterPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in characters, so this should throw an exception.
        CharacterPrimitiveTestObject obj = CharacterPrimitiveTestObject.createSync(dataManager, 'a');
        assertThrows(NullPointerException.class, () -> CharacterPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testBytePersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in bytes, so this should throw an exception.
        BytePrimitiveTestObject obj = BytePrimitiveTestObject.createSync(dataManager, (byte) 1);
        assertThrows(NullPointerException.class, () -> BytePrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testShortPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in shorts, so this should throw an exception.
        ShortPrimitiveTestObject obj = ShortPrimitiveTestObject.createSync(dataManager, (short) 1);
        assertThrows(NullPointerException.class, () -> ShortPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testIntegerPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in integers, so this should throw an exception.
        IntegerPrimitiveTestObject obj = IntegerPrimitiveTestObject.createSync(dataManager, 1);
        assertThrows(NullPointerException.class, () -> IntegerPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testLongPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in longs, so this should throw an exception.
        LongPrimitiveTestObject obj = LongPrimitiveTestObject.createSync(dataManager, 1L);
        assertThrows(NullPointerException.class, () -> LongPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testFloatPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in floats, so this should throw an exception.
        FloatPrimitiveTestObject obj = FloatPrimitiveTestObject.createSync(dataManager, 1.0f);
        assertThrows(NullPointerException.class, () -> FloatPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testDoublePersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in doubles, so this should throw an exception.
        DoublePrimitiveTestObject obj = DoublePrimitiveTestObject.createSync(dataManager, 1.0);
        assertThrows(NullPointerException.class, () -> DoublePrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testBooleanPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are not allowed in booleans, so this should throw an exception.
        BooleanPrimitiveTestObject obj = BooleanPrimitiveTestObject.createSync(dataManager, true);
        assertThrows(NullPointerException.class, () -> BooleanPrimitiveTestObject.createSync(dataManager, null));

        assertThrows(NullPointerException.class, () -> obj.setValue(null));
    }

    @RetryingTest(5)
    public void testUUIDPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are allowed in UUIDs, so this should be fine.
        UUIDPrimitiveTestObject obj = UUIDPrimitiveTestObject.createSync(dataManager, UUID.randomUUID());
        UUIDPrimitiveTestObject.createSync(dataManager, null);

        obj.setValue(null); //This should not throw an NPE
    }

    @RetryingTest(5)
    public void testTimestampPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are allowed in timestamps, so this should be fine.
        TimestampPrimitiveTestObject obj = TimestampPrimitiveTestObject.createSync(dataManager, Timestamp.from(Instant.now()));
        TimestampPrimitiveTestObject.createSync(dataManager, null);

        obj.setValue(null); //This should not throw an NPE
    }

    @RetryingTest(5)
    public void testByteArrayPersistentValue() {
        MockEnvironment environment = getMockEnvironments().getFirst();
        DataManager dataManager = environment.dataManager();

        //Null values are allowed in byte arrays, so this should be fine.
        ByteArrayPrimitiveTestObject obj = ByteArrayPrimitiveTestObject.createSync(dataManager, new byte[]{1, 2, 3});
        ByteArrayPrimitiveTestObject.createSync(dataManager, null);

        obj.setValue(null); //This should not throw an NPE
    }

    //-----Test nullability END-----

    //-----Test encoders and decoders START-----

    @RetryingTest(5)
    public void testStringEncodersAndDecoders() {
        String value = "Hello, World";
        String encoded = Primitives.STRING.encode(value);
        String decoded = Primitives.STRING.decode(encoded);

        assertEquals("Hello, World", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testStringEncodersAndDecodersWithNull() {
        String value = null;
        String encoded = Primitives.STRING.encode(value);
        String decoded = Primitives.STRING.decode(encoded);

        assertNull(encoded);
        assertNull(decoded);
    }

    @RetryingTest(5)
    public void testCharacterEncodersAndDecoders() {
        char value = 'a';
        String encoded = Primitives.CHARACTER.encode(value);
        char decoded = Primitives.CHARACTER.decode(encoded);

        assertEquals("a", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testByteEncodersAndDecoders() {
        byte value = 1;
        String encoded = Primitives.BYTE.encode(value);
        byte decoded = Primitives.BYTE.decode(encoded);

        assertEquals("1", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testShortEncodersAndDecoders() {
        short value = 1;
        String encoded = Primitives.SHORT.encode(value);
        short decoded = Primitives.SHORT.decode(encoded);

        assertEquals("1", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testIntegerEncodersAndDecoders() {
        int value = 1;
        String encoded = Primitives.INTEGER.encode(value);
        int decoded = Primitives.INTEGER.decode(encoded);

        assertEquals("1", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testLongEncodersAndDecoders() {
        long value = 1;
        String encoded = Primitives.LONG.encode(value);
        long decoded = Primitives.LONG.decode(encoded);

        assertEquals("1", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testFloatEncodersAndDecoders() {
        float value = 1.0f;
        String encoded = Primitives.FLOAT.encode(value);
        float decoded = Primitives.FLOAT.decode(encoded);

        assertEquals("1.0", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testDoubleEncodersAndDecoders() {
        double value = 1.0;
        String encoded = Primitives.DOUBLE.encode(value);
        double decoded = Primitives.DOUBLE.decode(encoded);

        assertEquals("1.0", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testBooleanEncodersAndDecoders() {
        boolean value = true;
        String encoded = Primitives.BOOLEAN.encode(value);
        boolean decoded = Primitives.BOOLEAN.decode(encoded);

        assertEquals("true", encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testUUIDEncodersAndDecoders() {
        UUID value = UUID.randomUUID();
        String encoded = Primitives.UUID.encode(value);
        UUID decoded = Primitives.UUID.decode(encoded);

        assertEquals(value.toString(), encoded);
        assertEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testUUIDEncodersAndDecodersWithNull() {
        UUID value = null;
        String encoded = Primitives.UUID.encode(value);
        UUID decoded = Primitives.UUID.decode(encoded);

        assertNull(encoded);
        assertNull(decoded);
    }

    @RetryingTest(5)
    public void testTimestampEncodersAndDecoders() {
        String encoded = "1970-01-01T00:00:00+00:00"; //We get timezones in the PostgresListener in ISO-8601 format
        Timestamp decoded = Primitives.TIMESTAMP.decode(encoded);

        assertEquals(Timestamp.from(Instant.EPOCH), decoded);
        assertEquals(encoded, Primitives.TIMESTAMP.encode(decoded));
    }

    @RetryingTest(5)
    public void testTimestampEncodersAndDecodersWithNull() {
        Timestamp value = null;
        String encoded = Primitives.TIMESTAMP.encode(value);
        Timestamp decoded = Primitives.TIMESTAMP.decode(encoded);

        assertNull(encoded);
        assertNull(decoded);
    }

    @RetryingTest(5)
    public void testByteArrayEncodersAndDecoders() {
        byte[] value = "Hello, World".getBytes();
        String encoded = Primitives.BYTE_ARRAY.encode(value);
        byte[] decoded = Primitives.BYTE_ARRAY.decode(encoded);

        assertEquals("\\x48656c6c6f2c20576f726c64", encoded);
        assertArrayEquals(value, decoded);
    }

    @RetryingTest(5)
    public void testByteArrayEncodersAndDecodersWithNull() {
        byte[] value = null;
        String encoded = Primitives.BYTE_ARRAY.encode(value);
        byte[] decoded = Primitives.BYTE_ARRAY.decode(encoded);

        assertNull(encoded);
        assertNull(decoded);
    }
}