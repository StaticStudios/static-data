package net.staticstudios.data.mock.persistentcollection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.value.redis.CachedValue;

import java.util.UUID;

public class FacebookUser extends UniqueData {
    public final CachedValue<Integer> followingAdditions = CachedValue.of(this, Integer.class, "following_adds").withFallback(0);
    public final CachedValue<Integer> postAdditions = CachedValue.of(this, Integer.class, "post_adds").withFallback(0);
    public final CachedValue<Integer> favoriteQuoteAdditions = CachedValue.of(this, Integer.class, "favorite_quote_adds").withFallback(0);
    public final CachedValue<Integer> followingRemovals = CachedValue.of(this, Integer.class, "following_removes").withFallback(0);
    public final CachedValue<Integer> postRemovals = CachedValue.of(this, Integer.class, "post_removes").withFallback(0);
    public final CachedValue<Integer> favoriteQuoteRemovals = CachedValue.of(this, Integer.class, "favorite_quote_removes").withFallback(0);

    public final PersistentCollection<FacebookUser> following = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "user_id", "following_id")
            .onAdd(change -> followingAdditions.set(followingAdditions.get() + 1))
            .onRemove(change -> followingRemovals.set(followingRemovals.get() + 1));
    public final PersistentCollection<FacebookUser> followers = PersistentCollection.manyToMany(this, FacebookUser.class, "facebook", "user_following", "following_id", "user_id");
    public final PersistentCollection<FacebookPost> posts = PersistentCollection.oneToMany(this, FacebookPost.class, "facebook", "posts", "user_id")
            .onAdd(change -> postAdditions.set(postAdditions.get() + 1))
            .onRemove(change -> postRemovals.set(postRemovals.get() + 1));
    public final PersistentCollection<String> favoriteQuotes = PersistentCollection.of(this, String.class, "facebook", "favorite_quotes", "user_id", "quote")
            .onAdd(change -> favoriteQuoteAdditions.set(favoriteQuoteAdditions.get() + 1))
            .onRemove(change -> favoriteQuoteRemovals.set(favoriteQuoteRemovals.get() + 1));

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
