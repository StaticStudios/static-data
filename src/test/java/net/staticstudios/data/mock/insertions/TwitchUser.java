package net.staticstudios.data.mock.insertions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.BatchInsert;

import java.util.UUID;

public class TwitchUser extends UniqueData {
    public final PersistentValue<String> name = PersistentValue.of(this, String.class, "name");
    public final PersistentCollection<TwitchChatMessage> messages = PersistentCollection.oneToMany(this, TwitchChatMessage.class, "twitch", "chat_messages", "sender_id");

    private TwitchUser(DataManager dataManager, UUID id) {
        super(dataManager, "twitch", "users", id);
    }

    public static TwitchUser enqueueCreation(BatchInsert batchInsert, DataManager dataManager, String name) {
        TwitchUser user = new TwitchUser(dataManager, UUID.randomUUID());
        batchInsert.add(user, user.name.initial(name));

        return user;
    }
}
