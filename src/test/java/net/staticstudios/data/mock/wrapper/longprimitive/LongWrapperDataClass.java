package net.staticstudios.data.mock.wrapper.longprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_long")
public class LongWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<LongWrapper> value;
}

