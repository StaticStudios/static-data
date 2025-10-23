package net.staticstudios.data.mock.wrapper.uuidprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_uuid")
public class UUIDWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<UUIDWrapper> value;
}

