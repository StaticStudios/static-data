package net.staticstudios.data.mock.account;

import com.google.gson.Gson;
import net.staticstudios.data.ValueSerializer;

public class AccountSettingsValueSerializer implements ValueSerializer<AccountSettings, String> {
    private static final Gson GSON = new Gson();

    @Override
    public AccountSettings deserialize(String serialized) {
        if (serialized == null || serialized.isEmpty()) {
            return null;
        }
        return GSON.fromJson(serialized, AccountSettings.class);
    }

    @Override
    public String serialize(AccountSettings deserialized) {
        if (deserialized == null) {
            return null;
        }
        return GSON.toJson(deserialized);
    }

    @Override
    public Class<AccountSettings> getDeserializedType() {
        return AccountSettings.class;
    }

    @Override
    public Class<String> getSerializedType() {
        return String.class;
    }
}
