package net.staticstudios.data.messaging;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public record PersistentCollectionChangeMessage(
        UUID uniqueId,
        String address,
        List<Map<String, String>> values
) {
}
