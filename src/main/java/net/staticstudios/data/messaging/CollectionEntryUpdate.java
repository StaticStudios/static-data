package net.staticstudios.data.messaging;

import java.util.Map;

public record CollectionEntryUpdate(Map<String, String> oldValues, Map<String, String> newValues) {
}
