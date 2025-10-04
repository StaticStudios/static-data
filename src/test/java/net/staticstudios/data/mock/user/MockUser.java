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

    @Insert(InsertStrategy.PREFER_EXISTING)
    @Delete(DeleteStrategy.CASCADE)
    @ForeignColumn(name = "fav_color", table = "user_preferences", nullable = true, link = "id=user_id")
    public PersistentValue<String> favoriteColor;

    @Delete(DeleteStrategy.CASCADE)
    @OneToOne(link = "settings_id=user_id")
    public Reference<MockUserSettings> settings;

    @Insert(InsertStrategy.OVERWRITE_EXISTING)
    @Delete(DeleteStrategy.NO_ACTION)
    @DefaultValue("0")
    @ForeignColumn(name = "name_updates", table = "user_metadata", link = "id=user_id")
    public PersistentValue<Integer> nameUpdates;

    @DefaultValue("Unknown")
    @Column(name = "name", index = true)
    public PersistentValue<String> name = PersistentValue.of(this, String.class)
            .onUpdate(MockUser.class, (user, update) -> {
                user.nameUpdates.set(user.getNameUpdates() + 1);
            });

    @UpdateInterval(5000)
    @Column(name = "views", nullable = true)
    public PersistentValue<Integer> views;

    //todo: on delete we need to have an option to set null. No action will handle this actually.
    @Delete(DeleteStrategy.NO_ACTION)
    @OneToMany(link = "id=user_id")
    public PersistentCollection<MockUserSession> sessions;

    //todo: support ManyToMany

    //todo: support OneToMany Collections where the data type is not a uniquedata. in this case additional info about what table and schema to use will be required, since we will have to create this table.

    public int getNameUpdates() {
        return nameUpdates.get();
    }
}
