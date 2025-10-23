package net.staticstudios.data.mock.wrapper.integerprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_integer")
public class IntegerWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<IntegerWrapper> value;
}

