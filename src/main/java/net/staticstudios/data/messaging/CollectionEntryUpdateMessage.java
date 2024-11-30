package net.staticstudios.data.messaging;

public record CollectionEntryUpdateMessage(
        String collectionAddress,
        CollectionEntryUpdate update
) {
}