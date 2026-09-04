package net.staticstudios.data.benchmark.data;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "skyblock", table = "players")
public class SkyblockPlayer extends UniqueData {

    @IdColumn(name = "id")
    public PersistentValue<UUID> id;

    @Column(name = "name")
    public PersistentValue<String> name;

    @Column(name = "settings_id")
    public PersistentValue<UUID> settingsId;

    @OneToOne(link = "settings_id=id")
    public Reference<SkyblockPlayerSettings> settings;

    @Delete(DeleteStrategy.CASCADE)
    @ManyToMany(link = "id=id", joinTable = "player_friends")
    public PersistentCollection<SkyblockPlayer> friends;
}
