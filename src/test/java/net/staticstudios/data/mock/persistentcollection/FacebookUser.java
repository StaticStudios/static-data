package net.staticstudios.data.mock.persistentcollection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.collection.SimplePersistentCollection;

import java.util.Collection;
import java.util.UUID;

public class FacebookUser extends UniqueData {
    private final PersistentCollection<FacebookUser> following = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "user_id", "following_id");
    private final PersistentCollection<FacebookUser> followers = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "following_id", "user_id");
    private final SimplePersistentCollection<FacebookPost> posts = PersistentCollection.oneToMany(this, FacebookPost.class, "facebook", "posts", "user_id");
    private final SimplePersistentCollection<String> favoriteQuotes = PersistentCollection.of(this, String.class, "facebook", "favorite_quotes", "user_id", "quote");

    private FacebookUser(DataManager dataManager, UUID id) {
        super(dataManager, "facebook", "users", id);
    }

    public static FacebookUser createSync(DataManager dataManager) {
        FacebookUser user = new FacebookUser(dataManager, UUID.randomUUID());
        dataManager.insert(user);

        return user;
    }

    public Collection<FacebookPost> getPosts() {
        return posts;
    }

    public Collection<String> getFavoriteQuotes() {
        return favoriteQuotes;
    }

    public Collection<FacebookUser> getFollowing() {
        return following;
    }

    public Collection<FacebookUser> getFollowers() {
        return followers;
    }
}
