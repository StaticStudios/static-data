package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.value.ForeignPersistentValue;
import net.staticstudios.data.value.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.sql.Connection;
import java.util.UUID;

/**
 * A mock Discord user only has {@link PersistentValue}s and a {@link ForeignPersistentValue}.
 */
@Table("discord.users")
public class MockDiscordUser extends UniqueData {
    private UpdatedValue<String> lastNameUpdate;
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name", updated -> lastNameUpdate = updated);
    private UpdatedValue<Integer> lastMessagesSentUpdate;
    private final ForeignPersistentValue<Integer> messagesSent = ForeignPersistentValue.of(this, Integer.class, "discord.stats", "messages_sent", "discord.user_stats", "user_id", "stats_id", updated -> lastMessagesSentUpdate = updated);

    private MockDiscordUser() {
    }

    @Blocking
    public MockDiscordUser(DataManager dataManager, UUID id, String name) {
        super(dataManager, id);

        this.name.setInternal(name);

        dataManager.insert(this);
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public int getMessagesSent() {
        return messagesSent.get();
    }

    public void setMessagesSent(int messagesSent) {
        this.messagesSent.set(messagesSent);
    }

    public void incrementMessagesSent() {
        this.messagesSent.set(this.messagesSent.get() + 1);
    }

    public void setStatsId(UUID id) {
        try (Connection connection = getDataManager().getConnection()) {
            messagesSent.setForeignObject(connection, id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public UUID getStatsId() {
        return messagesSent.getForeignObjectId();
    }

    public UpdatedValue<String> getLastNamesUpdate() {
        return lastNameUpdate;
    }

    public UpdatedValue<Integer> getLastMessagesSentUpdate() {
        return lastMessagesSentUpdate;
    }

}
