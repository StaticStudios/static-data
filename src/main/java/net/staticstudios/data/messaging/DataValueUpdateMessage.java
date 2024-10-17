package net.staticstudios.data.messaging;

import java.util.UUID;

public record DataValueUpdateMessage(String parentTopLevelTable, UUID uniqueId, String address, String encodedValue) {
}
