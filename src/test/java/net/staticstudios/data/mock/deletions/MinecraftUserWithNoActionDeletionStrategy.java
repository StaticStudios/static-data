package net.staticstudios.data.mock.deletions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DeletionStrategy;
import net.staticstudios.data.data.Reference;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.value.persistent.PersistentValue;
import net.staticstudios.data.data.value.redis.CachedValue;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public class MinecraftUserWithNoActionDeletionStrategy extends UniqueData {
    public final CachedValue<String> ipAddress = CachedValue.of(this, String.class, "ip")
            .deletionStrategy(DeletionStrategy.NO_ACTION);
    public final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    public final PersistentValue<Timestamp> accountCreation = PersistentValue.foreign(this, Timestamp.class, "minecraft.user_meta.account_creation", "id")
            .withDefault(Timestamp.from(Instant.now()))
            .deletionStrategy(DeletionStrategy.NO_ACTION);
    public final PersistentCollection<MinecraftServer> servers = PersistentCollection.manyToMany(this, MinecraftServer.class, "minecraft", "user_servers", "user_id", "server_id")
            .deletionStrategy(DeletionStrategy.NO_ACTION);
    public final PersistentCollection<MinecraftSkin> skins = PersistentCollection.oneToMany(this, MinecraftSkin.class, "minecraft", "skins", "user_id")
            .deletionStrategy(DeletionStrategy.NO_ACTION);
    public final Reference<MinecraftUserStatistics> statistics = Reference.of(this, MinecraftUserStatistics.class, "id")
            .deletionStrategy(DeletionStrategy.NO_ACTION);
    public final PersistentCollection<String> worldNames = PersistentCollection.of(this, String.class, "minecraft", "worlds", "user_id", "name")
            .deletionStrategy(DeletionStrategy.NO_ACTION);

    private MinecraftUserWithNoActionDeletionStrategy(DataManager dataManager, UUID id) {
        super(dataManager, "minecraft", "users", id);
    }

    public static MinecraftUserWithNoActionDeletionStrategy createSync(DataManager dataManager, String name, String ipAddress) {
        MinecraftUserWithNoActionDeletionStrategy user = new MinecraftUserWithNoActionDeletionStrategy(dataManager, UUID.randomUUID());
        MinecraftUserStatistics stats = MinecraftUserStatistics.createSync(dataManager, user.getId());
        dataManager.insert(user, user.name.initial(name), user.ipAddress.initial(ipAddress), user.statistics.initial(stats));

        return user;
    }
}
