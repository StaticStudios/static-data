package net.staticstudios.data.mocks.spotify;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.UpdatedValue;
import net.staticstudios.data.value.PersistentValue;
import org.jetbrains.annotations.Blocking;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * A mock Spotify user only has simple {@link PersistentValue}s, nothing fancy.
 */
@Table("spotify.users")
public class MockSpotifyUser extends UniqueData {
    private UpdatedValue<String> lastNameUpdate = null;
    private final PersistentValue<String> name = PersistentValue.of(this, String.class, "name", updated -> lastNameUpdate = updated);
    private UpdatedValue<Integer> lastMinutesListenedUpdate = null;
    private final PersistentValue<Integer> minutesListened = PersistentValue.withDefault(this, Integer.class, 0, "minutes_listened", updated -> lastMinutesListenedUpdate = updated);
    private UpdatedValue<Timestamp> lastCreatedAtUpdate = null;
    private final PersistentValue<Timestamp> createdAt = PersistentValue.supplyDefault(this, Timestamp.class, () -> Timestamp.from(Instant.now()), "created_at", updated -> lastCreatedAtUpdate = updated);

    @SuppressWarnings("unused")
    private MockSpotifyUser() {
    }

    @Blocking
    public MockSpotifyUser(DataManager dataManager, String name) {
        super(dataManager, UUID.randomUUID());

        this.name.setInternal(name);

        dataManager.insert(this);
    }

    public String getName() {
        return name.get();
    }

    public void setName(String name) {
        this.name.set(name);
    }

    public int getMinutesListened() {
        return minutesListened.get();
    }

    public void setMinutesListened(int minutesListened) {
        this.minutesListened.set(minutesListened);
    }

    public Timestamp getCreatedAt() {
        return createdAt.get();
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt.set(createdAt);
    }


    public UpdatedValue<String> getLastNameUpdate() {
        return lastNameUpdate;
    }

    public UpdatedValue<Integer> getLastMinutesListenedUpdate() {
        return lastMinutesListenedUpdate;
    }

    public UpdatedValue<Timestamp> getLastCreatedAtUpdate() {
        return lastCreatedAtUpdate;
    }
}
