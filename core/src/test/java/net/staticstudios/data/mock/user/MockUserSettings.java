package net.staticstudios.data.mock.user;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "public", table = "user_settings")
public class MockUserSettings extends UniqueData {
    @IdColumn(name = "user_id")
    public PersistentValue<UUID> id;
    @DefaultValue("10")
    @Column(name = "font_size")
    public PersistentValue<Integer> fontSize;
}
