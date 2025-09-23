package net.staticstudios.data.mock.wrapper.bytearrayprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_bytea")
public class ByteArrayWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<ByteArrayWrapper> value;
}

