package net.staticstudios.data.mock;

import net.staticstudios.data.*;

import java.util.UUID;

@Data(schema = "public", table = "users")
public class MockUser extends UniqueData {
    //todo: @OneToMany, @ManyToMany, @ManyToOne

    @IdColumn(name = "id")
    public PersistentValue<UUID> id = PersistentValue.of(this, UUID.class);
    @Column(name = "settings_id", nullable = true)
    public PersistentValue<UUID> settingsId = PersistentValue.of(this, UUID.class);
    @Column(name = "age", nullable = true)
    public PersistentValue<Integer> age;
    @ForeignColumn(name = "fav_color", table = "user_preferences", nullable = true, link = "id=user_id")
    public PersistentValue<String> favoriteColor;
    @OneToOne(link = "settings_id=user_id")
    public Reference<MockUserSettings> settings;
    @ForeignColumn(name = "name_updates", table = "user_metadata", link = "id=user_id")
    public PersistentValue<Integer> nameUpdates;
    @Column(name = "name", index = true)
    public PersistentValue<String> name = PersistentValue.of(this, String.class)
            .onUpdate(MockUser.class, (user, update) -> {
                //todo: nameupdates shouldnt be null but until we get default values implemented we have to do this
                user.nameUpdates.set(user.nameUpdates.get() == null ? 1 : user.nameUpdates.get() + 1);
            })
            .withDefault("Unknown");

    //todo: add support for unique constraints and test them.
    //todo: enfore the nullable constraint. actually - it might fail for us via H2. test this.
}
