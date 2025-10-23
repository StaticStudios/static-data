package net.staticstudios.data.mock.wrapper.doubleprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_double")
public class DoubleWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<DoubleWrapper> value;
}

