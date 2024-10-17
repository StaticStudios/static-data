package net.staticstudios.data.messaging;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.staticstudios.data.messaging.json.PersistentCollectionChangeMessageTypeAdapter;

public class DataMessageUtils {
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(PersistentCollectionChangeMessage.class, new PersistentCollectionChangeMessageTypeAdapter())
            .create();

    public static Gson getGson() {
        return gson;
    }
}
