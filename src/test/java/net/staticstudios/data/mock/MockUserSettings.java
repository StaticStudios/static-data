package net.staticstudios.data.mock;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "public", table = "user_settings")
public class MockUserSettings extends UniqueData {
    @IdColumn(name = "user_id")
    public PersistentValue<UUID> id;
    @Column(name = "font_size", defaultValue = "10")
    public PersistentValue<Integer> fontSide;

    public static MockUserSettings create(DataManager dataManager, UUID id) {
        return dataManager.createInsertContext()
                .set(MockUserSettings.class, "user_id", id)
                .insert(InsertMode.SYNC)
                .get(MockUserSettings.class);
    }
}
