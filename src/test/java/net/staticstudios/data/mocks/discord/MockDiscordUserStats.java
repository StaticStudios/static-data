package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.value.ForeignPersistentValue;
import net.staticstudios.data.value.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

/**
 * A mock Discord user stats object only has {@link PersistentValue}s and a {@link ForeignPersistentValue}.
 */
@Table("discord.stats")
public class MockDiscordUserStats extends UniqueData {
    private final ForeignPersistentValue<String> favoriteColor = ForeignPersistentValue.of(this, String.class, "discord.users", "favorite_color", "discord.user_stats", "stats_id", "user_id");
    private final PersistentValue<Integer> favoriteNumber = PersistentValue.withDefault(this, Integer.class, 0, "favorite_number");
    private UpdatedValue<String> lastNameUpdate;
    private final ForeignPersistentValue<String> name = ForeignPersistentValue.of(this, String.class, "discord.users", "name", "discord.user_stats", "stats_id", "user_id", updated -> lastNameUpdate = updated);
    private UpdatedValue<Integer> lastMessagesSentUpdate;
    private final PersistentValue<Integer> messagesSent = PersistentValue.withDefault(this, Integer.class, 0, "messages_sent", updated -> lastMessagesSentUpdate = updated);

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

    public String getUserName() {
        return name.get();
    }

    public UUID getUserId() {
        return name.getForeignObjectId();
    }

    public void setUserName(String name) {
        this.name.set(name);
    }

    public UpdatedValue<String> getLastUserNamesUpdate() {
        return lastNameUpdate;
    }

    public UpdatedValue<Integer> getLastMessagesSentUpdate() {
        return lastMessagesSentUpdate;
    }

    public String getUserFavoriteColor() {
        return favoriteColor.get();
    }

    public void setUserFavoriteColor(String favoriteColor) {
        this.favoriteColor.set(favoriteColor);
    }

    public int getFavoriteNumber() {
        return favoriteNumber.get();
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber.set(favoriteNumber);
    }

    public ForeignPersistentValue<String> getUserFavoriteColorFPV() {
        return favoriteColor;
    }

}
