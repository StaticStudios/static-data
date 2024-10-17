package net.staticstudios.data.messaging;

import java.util.UUID;

public record CollectionEntryUpdateMessage(
        UUID uniqueId,
        String collectionAddress,
        CollectionEntryUpdate update
) {
}