package net.staticstudios.data.data.collection;

import java.util.UUID;

/**
 * Represents an entry in a collection.
 *
 * @param id    The id of the entry, note that this is not the same as the holder's id
 * @param value The value of the entry
 */
public record CollectionEntry(UUID id, Object value) {
}
