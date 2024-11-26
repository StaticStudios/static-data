package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.ForeignPersistentValue;
import net.staticstudios.data.value.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

/**
 * A mock Discord user stats object only has {@link PersistentValue}s and a {@link ForeignPersistentValue}.
 */
@Table("discord.stats")
public class MockDiscordUserStats extends UniqueData {
    private final PersistentValue<Integer> messagesSent = PersistentValue.withDefault(this, Integer.class, 0, "messages_sent");
    private final ForeignPersistentValue<String> name = ForeignPersistentValue.of(this, String.class, "discord.users", "name", "discord.user_stats", "stats_id", "user_id");

    private MockDiscordUserStats() {
    }

    @Blocking
    public MockDiscordUserStats(DataManager dataManager, UUID id) {
        super(dataManager, id);

        dataManager.insert(this);
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

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

}
