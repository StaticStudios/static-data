package net.staticstudios.data.messaging;

import java.util.UUID;

public record ForeignLinkUpdateMessage(String linkingTable, UUID id1, UUID id2,
                                       String column1, String column2) {
}
