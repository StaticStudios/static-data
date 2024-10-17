package net.staticstudios.data.mocks;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.PersistentValue;

import java.sql.Timestamp;
import java.util.UUID;

@Table("public.test_data_types")
public class MockTestDataObject extends UniqueData {
    // Have a PersistentValue for every DatabaseSupportedType
    private final PersistentValue<String> testString = PersistentValue.withDefault(this, String.class, "test", "test_string");
    private final PersistentValue<Character> testChar = PersistentValue.withDefault(this, Character.class, 'c', "test_char");
    private final PersistentValue<Short> testShort = PersistentValue.withDefault(this, Short.class, (short) 1, "test_short");
    private final PersistentValue<Integer> testInt = PersistentValue.withDefault(this, Integer.class, 1, "test_int");
    private final PersistentValue<Long> testLong = PersistentValue.withDefault(this, Long.class, 1L, "test_long");
    private final PersistentValue<Float> testFloat = PersistentValue.withDefault(this, Float.class, 1.0f, "test_float");
    private final PersistentValue<Double> testDouble = PersistentValue.withDefault(this, Double.class, 1.0, "test_double");
    private final PersistentValue<Boolean> testBoolean = PersistentValue.withDefault(this, Boolean.class, true, "test_boolean");
    private final PersistentValue<UUID> testUUID = PersistentValue.withDefault(this, UUID.class, UUID.randomUUID(), "test_uuid");
    private final PersistentValue<Timestamp> testTimestamp = PersistentValue.withDefault(this, Timestamp.class, new Timestamp(0), "test_timestamp");
    private final PersistentValue<byte[]> testByteArray = PersistentValue.withDefault(this, byte[].class, new byte[]{1, 2, 3}, "test_byte_array");
    private final PersistentValue<UUID[]> testUUIDArray = PersistentValue.withDefault(this, UUID[].class, new UUID[]{UUID.randomUUID()}, "test_uuid_array");

    @SuppressWarnings("unused")
    private MockTestDataObject() {
    }

    public MockTestDataObject(DataManager dataManager) {
        super(dataManager, UUID.randomUUID());
        dataManager.insert(this);
    }

    public String getString() {
        return testString.get();
    }

    public void setString(String testString) {
        this.testString.set(testString);
    }

    public char getChar() {
        return testChar.get();
    }

    public void setChar(char testChar) {
        this.testChar.set(testChar);
    }

    public short getShort() {
        return testShort.get();
    }

    public void setShort(short testShort) {
        this.testShort.set(testShort);
    }

    public int getInt() {
        return testInt.get();
    }

    public void setInt(int testInt) {
        this.testInt.set(testInt);
    }

    public long getLong() {
        return testLong.get();
    }

    public void setLong(long testLong) {
        this.testLong.set(testLong);
    }

    public float getFloat() {
        return testFloat.get();
    }

    public void setFloat(float testFloat) {
        this.testFloat.set(testFloat);
    }

    public double getDouble() {
        return testDouble.get();
    }

    public void setDouble(double testDouble) {
        this.testDouble.set(testDouble);
    }

    public boolean getBoolean() {
        return testBoolean.get();
    }

    public void setBoolean(boolean testBoolean) {
        this.testBoolean.set(testBoolean);
    }

    public UUID getUuid() {
        return testUUID.get();
    }

    public void setUuid(UUID testUUID) {
        this.testUUID.set(testUUID);
    }

    public Timestamp getTimestamp() {
        return testTimestamp.get();
    }

    public void setTimestamp(Timestamp testTimestamp) {
        this.testTimestamp.set(testTimestamp);
    }

    public byte[] getByteArray() {
        return testByteArray.get();
    }

    public void setByteArray(byte[] testByteArray) {
        this.testByteArray.set(testByteArray);
    }

    public UUID[] getUuidArray() {
        return testUUIDArray.get();
    }

    public void setUuidArray(UUID[] testUUIDArray) {
        this.testUUIDArray.set(testUUIDArray);
    }
}
