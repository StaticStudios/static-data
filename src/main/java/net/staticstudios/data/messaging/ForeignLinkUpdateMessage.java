package net.staticstudios.data.messaging;

import java.util.UUID;

public record ForeignLinkUpdateMessage(String column, String linkingTable, UUID id1, UUID id2, UUID prevForeignId,
                                       String column1, String column2, Object value2) {
}
