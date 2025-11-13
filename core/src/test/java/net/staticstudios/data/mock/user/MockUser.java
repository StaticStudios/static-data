package net.staticstudios.data.mock.user;

import net.staticstudios.data.*;

import java.util.UUID;

//todo: heres how inheritance should look:
// if the super class provides a data annotation, ignore it and use the child's annotation. it would be cool tho to allow the super class to use a @data annotation. the former is whats implemented now. if changed, update the processor.
@Data(schema = "public", table = "users")
public class MockUser extends UniqueData {
    //todo: test inheritance properly. test the ij plugin and AP too.
    @IdColumn(name = "id")
    public PersistentValue<UUID> id = PersistentValue.of(this, UUID.class);

    @Column(name = "settings_id", nullable = true, unique = true)
    public PersistentValue<UUID> settingsId = PersistentValue.of(this, UUID.class);

    @Column(name = "age", nullable = true)
    public PersistentValue<Integer> age;

    @Insert(InsertStrategy.PREFER_EXISTING)
    @Delete(DeleteStrategy.CASCADE)
    @ForeignColumn(name = "fav_color", table = "user_preferences", nullable = true, link = "id=user_id")
    public PersistentValue<String> favoriteColor;

    @Identifier("settings_updates")
    public CachedValue<Integer> settingsUpdates = CachedValue.of(this, Integer.class)
            .withFallback(0);

    @Delete(DeleteStrategy.CASCADE)
    @OneToOne(link = "settings_id=user_id")
    public Reference<MockUserSettings> settings = Reference.of(this, MockUserSettings.class)
            .onUpdate(MockUser.class, (user, update) -> user.settingsUpdates.set(user.settingsUpdates.get() + 1));

    @Insert(InsertStrategy.OVERWRITE_EXISTING)
    @Delete(DeleteStrategy.NO_ACTION)
    @DefaultValue("0")
    @ForeignColumn(name = "name_updates", table = "user_metadata", link = "id=user_id")
    public PersistentValue<Integer> nameUpdates;

    @DefaultValue("Unknown")
    @Column(name = "name", index = true)
    public PersistentValue<String> name = PersistentValue.of(this, String.class)
            .onUpdate(MockUser.class, (user, update) -> user.nameUpdates.set(user.getNameUpdates() + 1));

    @UpdateInterval(5000)
    @Column(name = "views", nullable = true)
    public PersistentValue<Integer> views;

    @Identifier("session_additions")
    public CachedValue<Integer> sessionAdditions = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Identifier("session_removals")
    public CachedValue<Integer> sessionRemovals = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Delete(DeleteStrategy.NO_ACTION)
    @OneToMany(link = "id=user_id")
    public PersistentCollection<MockUserSession> sessions = PersistentCollection.of(this, MockUserSession.class)
            .onAdd(MockUser.class, (user, added) -> user.sessionAdditions.set(user.sessionAdditions.get() + 1))
            .onRemove(MockUser.class, (user, removed) -> user.sessionRemovals.set(user.sessionRemovals.get() + 1));


    @Identifier("friend_additions")
    public CachedValue<Integer> friendAdditions = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Identifier("friend_removals")
    public CachedValue<Integer> friendRemovals = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Delete(DeleteStrategy.CASCADE) //todo: impl delete strategy for many to many collections
    @ManyToMany(link = "id=id", joinTable = "user_friends")
    public PersistentCollection<MockUser> friends = PersistentCollection.of(this, MockUser.class)
            .onAdd(MockUser.class, (user, added) -> user.friendAdditions.set(user.friendAdditions.get() + 1))
            .onRemove(MockUser.class, (user, removed) -> user.friendRemovals.set(user.friendRemovals.get() + 1));


    @Identifier("favorite_number_additions")
    public CachedValue<Integer> favoriteNumberAdditions = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Identifier("favorite_number_removals")
    public CachedValue<Integer> favoriteNumberRemovals = CachedValue.of(this, Integer.class)
            .withFallback(0);

    @Delete(DeleteStrategy.CASCADE)
    @OneToMany(link = "id=user_id", table = "favorite_numbers", column = "number")
    public PersistentCollection<Integer> favoriteNumbers = PersistentCollection.of(this, Integer.class)
            .onAdd(MockUser.class, (user, added) -> user.favoriteNumberAdditions.set(user.favoriteNumberAdditions.get() + 1))
            .onRemove(MockUser.class, (user, removed) -> user.favoriteNumberRemovals.set(user.favoriteNumberRemovals.get() + 1));
    @Identifier("cooldown_updates")
    public CachedValue<Integer> cooldownUpdates = CachedValue.of(this, Integer.class)
            .withFallback(0);
    @Identifier("on_cooldown")
    @ExpireAfter(5)
    public CachedValue<Boolean> onCooldown = CachedValue.of(this, Boolean.class)
            .onUpdate(MockUser.class, (user, update) -> user.cooldownUpdates.set(user.cooldownUpdates.get() + 1))
            .withFallback(false);

    public int getNameUpdates() {
        return nameUpdates.get();
    }
}
