package net.staticstudios.data.mock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.insert.InsertMode;
import net.staticstudios.data.parse.Column;
import net.staticstudios.data.parse.Data;
import net.staticstudios.data.util.IdColumn;

import java.util.UUID;

@Data(schema = "public", table = "user_settings")
public class MockUserSettings extends UniqueData {
    @IdColumn(name = "user_id")
    public PersistentValue<UUID> id;
    @Column(name = "font_size")
    public PersistentValue<Integer> fontSide;

    public static MockUserSettings create(DataManager dataManager, UUID id) {
        return dataManager.createInsertContext()
                .set(MockUserSettings.class, "user_id", id)
                //todo: enfore the nullable constraint. actually - it might fail for us via H2. test this.
                .insert(InsertMode.SYNC)
                .get(MockUserSettings.class);
    }
}
