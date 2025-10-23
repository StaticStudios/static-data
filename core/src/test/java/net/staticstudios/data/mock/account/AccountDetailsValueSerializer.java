package net.staticstudios.data.mock.account;

import com.google.gson.Gson;
import net.staticstudios.data.ValueSerializer;
import org.jetbrains.annotations.NotNull;

public class AccountDetailsValueSerializer implements ValueSerializer<AccountDetails, String> {
    private static final Gson GSON = new Gson();

    @Override
    public AccountDetails deserialize(@NotNull String serialized) {
        if (serialized == null || serialized.isEmpty()) {
            return null;
        }
        return GSON.fromJson(serialized, AccountDetails.class);
    }

    @Override
    public String serialize(@NotNull AccountDetails deserialized) {
        if (deserialized == null) {
            return null;
        }
        return GSON.toJson(deserialized);
    }

    @Override
    public Class<AccountDetails> getDeserializedType() {
        return AccountDetails.class;
    }

    @Override
    public Class<String> getSerializedType() {
        return String.class;
    }
}
