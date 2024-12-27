package net.staticstudios.data.mock.persistentcollection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.collection.SimplePersistentCollection;

import java.util.UUID;

public class FacebookUser extends UniqueData {
    public final PersistentCollection<FacebookUser> following = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "user_id", "following_id");
    public final PersistentCollection<FacebookUser> followers = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "following_id", "user_id");
    public final SimplePersistentCollection<FacebookPost> posts = PersistentCollection.oneToMany(this, FacebookPost.class, "facebook", "posts", "user_id");
    public final SimplePersistentCollection<String> favoriteQuotes = PersistentCollection.of(this, String.class, "facebook", "favorite_quotes", "user_id", "quote");

    private FacebookUser(DataManager dataManager, UUID id) {
        super(dataManager, "facebook", "users", id);
    }

    public static FacebookUser createSync(DataManager dataManager) {
        FacebookUser user = new FacebookUser(dataManager, UUID.randomUUID());
        dataManager.insert(user);

        return user;
    }

    public PersistentCollection<FacebookPost> getPosts() {
        return posts;
    }

    public PersistentCollection<String> getFavoriteQuotes() {
        return favoriteQuotes;
    }

    public PersistentCollection<FacebookUser> getFollowing() {
        return following;
    }

    public PersistentCollection<FacebookUser> getFollowers() {
        return followers;
    }
}
