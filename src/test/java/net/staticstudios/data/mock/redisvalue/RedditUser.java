package net.staticstudios.data.mock.redisvalue;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.value.redis.RedisValue;

import java.sql.Timestamp;
import java.util.UUID;

public class RedditUser extends UniqueData {
    public final RedisValue<Integer> status_updates = RedisValue.of(this, Integer.class, "status_updates")
            .withFallback(0);
    public final RedisValue<String> status = RedisValue.of(this, String.class, "status")
            .onUpdate(update -> status_updates.set(status_updates.get() + 1));
    public final RedisValue<Timestamp> lastLogin = RedisValue.of(this, Timestamp.class, "last_login");
    public final RedisValue<Boolean> suspended = RedisValue.of(this, Boolean.class, "suspended")
            .withFallback(false)
            .withExpiry(3);
    public final RedisValue<String> bio = RedisValue.of(this, String.class, "bio")
            .withFallback("This user has not set a bio yet.");

    private RedditUser(DataManager dataManager, UUID id) {
        super(dataManager, "reddit", "users", id);
    }

    public static RedditUser createSync(DataManager dataManager) {
        RedditUser user = new RedditUser(dataManager, UUID.randomUUID());
        dataManager.insert(user);

        return user;
    }

    public String getStatus() {
        return status.get();
    }

    public void setStatus(String status) {
        this.status.set(status);
    }

    public int getStatusUpdates() {
        return status_updates.get();
    }

    public void setStatusUpdates(int statusUpdates) {
        this.status_updates.set(statusUpdates);
    }

    public Timestamp getLastLogin() {
        return lastLogin.get();
    }

    public void setLastLogin(Timestamp lastLogin) {
        this.lastLogin.set(lastLogin);
    }

    public boolean isSuspended() {
        return suspended.get();
    }

    public void setSuspended(boolean suspended) {
        this.suspended.set(suspended);
    }

    public String getBio() {
        return bio.get();
    }

    public void setBio(String bio) {
        this.bio.set(bio);
    }
}
