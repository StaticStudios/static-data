package net.staticstudios.data.mock;

import net.staticstudios.data.*;

import java.util.UUID;

//todo: heres how inheritance should look:
// if the super class provides a data annotation, ignore it and use the child's annotation. it would be cool tho to allow the super class to use a @data annotation. the former is whats implemented now. if changed, update the processor.

@Data(schema = "public", table = "users")
public class MockUser extends UniqueData {
    //todo: @OneToMany, @ManyToMany, @ManyToOne

    //todo: if nullable is false, find the sql type and set a reasonable default.
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
                user.nameUpdates.set(user.getNameUpdates() + 1);
            })
            .withDefault("Unknown");

    //todo: add support for unique constraints and test them.
    //todo: enfore the nullable constraint. actually - it might fail for us via H2. test this.

    public int getNameUpdates() {
        //todo: nameupdates shouldnt be null but until we get default values implemented we have to do this
        return nameUpdates.get() == null ? 0 : nameUpdates.get();
    }

}
