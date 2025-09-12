package net.staticstudios.data.mock.insertions;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.BatchInsert;

import java.util.UUID;

public class TwitchChatMessage extends UniqueData {
    public final Reference<TwitchUser> sender = Reference.of(this, TwitchUser.class, "sender_id");

    private TwitchChatMessage(DataManager dataManager, UUID id) {
        super(dataManager, "twitch", "chat_messages", id);
    }

    public static TwitchChatMessage enqueueCreation(BatchInsert batchInsert, DataManager dataManager, TwitchUser sender) {
        TwitchChatMessage message = new TwitchChatMessage(dataManager, UUID.randomUUID());
        batchInsert.add(message, message.sender.initial(sender));

        return message;
    }
}
