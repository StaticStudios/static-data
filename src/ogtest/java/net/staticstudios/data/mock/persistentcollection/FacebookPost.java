package net.staticstudios.data.mock.persistentcollection;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.Reference;
import net.staticstudios.data.UniqueData;

import java.util.UUID;

public class FacebookPost extends UniqueData {
    private final PersistentValue<String> description = PersistentValue.of(this, String.class, "description");
    private final PersistentValue<Integer> likes = PersistentValue.of(this, Integer.class, "likes")
            .withDefault(0);
    private final Reference<FacebookUser> user = Reference.of(this, FacebookUser.class, "user_id");

    private FacebookPost(DataManager dataManager, UUID id) {
        super(dataManager, "facebook", "posts", id);
    }

    public static FacebookPost createSync(DataManager dataManager, String description, FacebookUser user) {
        FacebookPost post = new FacebookPost(dataManager, UUID.randomUUID());

        dataManager.insert(post, post.description.initial(description), post.user.initial(user));

        return post;
    }

    public String getDescription() {
        return description.get();
    }

    public void setDescription(String description) {
        this.description.set(description);
    }

    public Integer getLikes() {
        return likes.get();
    }

    public void setLikes(Integer likes) {
        this.likes.set(likes);
    }

    public FacebookUser getUser() {
        return user.get();
    }

    public void setUser(FacebookUser user) {
        this.user.set(user);
    }
}
