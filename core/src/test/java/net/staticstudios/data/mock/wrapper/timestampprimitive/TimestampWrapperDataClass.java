package net.staticstudios.data.mock.wrapper.timestampprimitive;

import net.staticstudios.data.*;

@Data(schema = "public", table = "custom_type_test_timestamp")
public class TimestampWrapperDataClass extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "val", nullable = true)
    public PersistentValue<TimestampWrapper> value;
}

