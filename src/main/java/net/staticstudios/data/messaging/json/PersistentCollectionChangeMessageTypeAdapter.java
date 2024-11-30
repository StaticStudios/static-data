package net.staticstudios.data.messaging.json;

import com.google.common.base.Preconditions;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.staticstudios.data.messaging.DataMessageUtils;
import net.staticstudios.data.messaging.PersistentCollectionChangeMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PersistentCollectionChangeMessageTypeAdapter extends TypeAdapter<PersistentCollectionChangeMessage> {

    public static final TypeToken<List<Map<String, String>>> LIST_STRING_STRING_MAP = new TypeToken<>() {
    };

    @Override
    public void write(JsonWriter out, PersistentCollectionChangeMessage value) throws IOException {
        out.beginObject();


        out.name("collection_address");
        out.value(value.collectionAddress());


        TypeAdapter<List<Map<String, String>>> adapter = DataMessageUtils.getGson().getAdapter(LIST_STRING_STRING_MAP);

        out.name("values");
        adapter.write(out, value.values());

        out.endObject();
    }

    @Override
    public PersistentCollectionChangeMessage read(JsonReader in) throws IOException {
        in.beginObject();

        String collectionAddress = null;
        String uniqueId = null;
        List<Map<String, String>> values = null;

        while (in.hasNext()) {
            JsonToken token = in.peek();
            String fieldname = null;

            if (token.equals(JsonToken.NAME)) {
                //get the current token
                fieldname = in.nextName();
            }

            if ("collection_address".equals(fieldname)) {
                token = in.peek();
                if (token.equals(JsonToken.STRING)) {
                    collectionAddress = in.nextString();
                    continue;
                }
            }

            if ("values".equals(fieldname)) {
                token = in.peek();
                if (token.equals(JsonToken.BEGIN_ARRAY)) {
                    TypeAdapter<List<Map<String, String>>> adapter = DataMessageUtils.getGson().getAdapter(LIST_STRING_STRING_MAP);
                    values = adapter.read(in);
                }
            }
        }

        in.endObject();

        Preconditions.checkNotNull(collectionAddress, "collection_address cannot be null");
        Preconditions.checkNotNull(values, "values cannot be null");

        return new PersistentCollectionChangeMessage(collectionAddress, values);
    }
}
