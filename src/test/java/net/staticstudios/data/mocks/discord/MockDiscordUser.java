package net.staticstudios.data.mocks.discord;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.value.ForeignPersistentValue;
import net.staticstudios.data.value.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.sql.Connection;
import java.util.Objects;
import java.util.UUID;

/**
 * A mock Discord user only has {@link PersistentValue}s and a {@link ForeignPersistentValue}.
 */
@Table("discord.users")
public class MockDiscordUser extends UniqueData {
    private final PersistentValue<String> favoriteColor = PersistentValue.withDefault(this, String.class, "red", "favorite_color");
    private final ForeignPersistentValue<Integer> favoriteNumber = ForeignPersistentValue.of(this, Integer.class, "discord.stats", "favorite_number", "discord.user_stats", "user_id", "stats_id");
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

    public int getStatsMessagesSent() {
        Integer messagesSent = this.messagesSent.get();
        return messagesSent == null ? 0 : messagesSent;
    }

    public void setStatsMessagesSent(int messagesSent) {
        this.messagesSent.set(messagesSent);
    }

    public void incrementStatsMessagesSent() {
        this.messagesSent.set(Objects.requireNonNull(this.messagesSent.get()) + 1);
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

    public String getFavoriteColor() {
        return favoriteColor.get();
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor.set(favoriteColor);
    }

    public int getStatsFavoriteNumber() {
        Integer favoriteNumber = this.favoriteNumber.get();
        return favoriteNumber == null ? 0 : favoriteNumber;
    }

    public void setStatsFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber.set(favoriteNumber);
    }

    public ForeignPersistentValue<Integer> getStatsFavoriteNumberFPV() {
        return favoriteNumber;
    }

}
