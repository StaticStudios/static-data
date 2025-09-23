package net.staticstudios.data.mock.wrapper.stringprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test")
public class StringWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<StringWrapper> value;
}