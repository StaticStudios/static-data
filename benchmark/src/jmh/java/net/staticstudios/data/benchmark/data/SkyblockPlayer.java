package net.staticstudios.data.benchmark.data;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "skyblock", table = "players")
public class SkyblockPlayer extends UniqueData {

    @IdColumn(name = "id")
    public PersistentValue<UUID> id;


    @Column(name = "name")
    public PersistentValue<String> name;
}
