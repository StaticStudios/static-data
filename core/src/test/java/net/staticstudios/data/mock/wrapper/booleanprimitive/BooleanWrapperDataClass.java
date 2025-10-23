package net.staticstudios.data.mock.wrapper.booleanprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_boolean")
public class BooleanWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<BooleanWrapper> value;
}

