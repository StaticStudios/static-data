package net.staticstudios.data.mock.wrapper.floatprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_float")
public class FloatWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<FloatWrapper> value;
}

