package net.staticstudios.data.benchmark.data;

import net.staticstudios.data.Column;
import net.staticstudios.data.Data;
import net.staticstudios.data.IdColumn;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

@Data(schema = "skyblock", table = "player_settings")
public class SkyblockPlayerSettings extends UniqueData {

    @IdColumn(name = "id")
    public PersistentValue<UUID> id;

    @Column(name = "tablist_priority")
    public PersistentValue<Integer> tablistPriority;
}
