package net.staticstudios.data.mock.user;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "public", table = "user_settings")
public class MockUserSettings extends UniqueData {
    @IdColumn(name = "user_id")
    public PersistentValue<UUID> id;
    @Column(name = "font_size", defaultValue = "10")
    public PersistentValue<Integer> fontSize;
}
