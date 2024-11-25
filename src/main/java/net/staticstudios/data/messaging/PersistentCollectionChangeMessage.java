package net.staticstudios.data.messaging;

import java.util.List;
import java.util.Map;

public record PersistentCollectionChangeMessage(
        String collectionAddress,
        List<Map<String, String>> values
) {
}
