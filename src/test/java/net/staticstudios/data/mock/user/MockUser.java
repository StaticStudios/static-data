package net.staticstudios.data.mock.user;

import net.staticstudios.data.*;

import java.util.UUID;

//todo: heres how inheritance should look:
// if the super class provides a data annotation, ignore it and use the child's annotation. it would be cool tho to allow the super class to use a @data annotation. the former is whats implemented now. if changed, update the processor.

@Data(schema = "public", table = "users")
public class MockUser extends UniqueData {
    //todo: cached values
    //todo: @OneToMany, @ManyToMany, @ManyToOne
    //todo: note - maybe PC's add and remove handlers can be implemented using update handlers
    @IdColumn(name = "id")
    public PersistentValue<UUID> id = PersistentValue.of(this, UUID.class);
    @Column(name = "settings_id", nullable = true, unique = true)
    public PersistentValue<UUID> settingsId = PersistentValue.of(this, UUID.class);
    @Column(name = "age", nullable = true)
    public PersistentValue<Integer> age;
    @ForeignColumn(name = "fav_color", table = "user_preferences", nullable = true, link = "id=user_id")
    public PersistentValue<String> favoriteColor;
    @OneToOne(link = "settings_id=user_id", deleteStrategy = DeleteStrategy.CASCADE)
    public Reference<MockUserSettings> settings;
    @ForeignColumn(name = "name_updates", table = "user_metadata", link = "id=user_id", defaultValue = "0")
    public PersistentValue<Integer> nameUpdates;
    @Column(name = "name", index = true, defaultValue = "Unknown")
    public PersistentValue<String> name = PersistentValue.of(this, String.class)
            .onUpdate(MockUser.class, (user, update) -> {
                user.nameUpdates.set(user.getNameUpdates() + 1);
            });

    public int getNameUpdates() {
        return nameUpdates.get();
    }

}
