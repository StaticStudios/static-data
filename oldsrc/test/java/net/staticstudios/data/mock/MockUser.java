package net.staticstudios.data.mock;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.annotations.Data;
import net.staticstudios.data.insert.InsertMode;
import net.staticstudios.data.parse.Column;
import net.staticstudios.data.util.ForeignColumn;
import net.staticstudios.data.util.IdColumn;
import net.staticstudios.data.util.OneToOne;

import java.util.UUID;

@Data(schema = "public", table = "users")
public class MockUser extends UniqueData {
    //todo: @OneToMany, @ManyToMany, @ManyToOne


    @IdColumn(name = "id")
    public PersistentValue<UUID> id = PersistentValue.of(this, UUID.class);
    @Column(name = "settings_id")
    public PersistentValue<UUID> settingsId = PersistentValue.of(this, UUID.class);
    @Column(name = "age", nullable = true)
    public PersistentValue<Integer> age;
    @ForeignColumn(name = "fav_color", table = "user_preferences", nullable = true, link = "id=user_id")
    public PersistentValue<String> favoriteColor;
    @OneToOne(link = "settings_id=user_id")
    public Reference<MockUserSettings> settings;
    @Column(name = "name", index = true)
    public PersistentValue<String> name = PersistentValue.of(this, String.class)
            .onUpdate(MockUser.class, (user, update) -> {
//                System.out.println("User " + user.id.get() + " changed name from " + update.oldValue() + " to " + update.newValue());
            })
            .withDefault("Unknown");

    public static MockUser create(DataManager dataManager, UUID id, String name) {
        return dataManager.createInsertContext()
                .set(MockUser.class, "id", id) //todo: i dislike that we lose type safety
                .set(MockUser.class, "name", name)
                //todo: add support for unique constraints and test them.
                //todo: enfore the nullable constraint. actually - it might fail for us via H2. test this.
                .insert(InsertMode.SYNC)
                .get(MockUser.class);
        

        //TODO: generate the following patterns at compile time from the @Data annotation.

        /* MockUser.builder(datamanager)
         * .id(id)
         * .name(name)
         * .insert(InsertMode.SYNC); //returns the inserted object
         */
        /* MockUser.builder(datamanager)
         * .id(id)
         * .name(name)
         * .insert(insertContext); //add the object to an existing insert context, and return void.
         */
    }
}
