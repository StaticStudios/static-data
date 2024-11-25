package net.staticstudios.data.messaging;

import org.jetbrains.annotations.NotNull;

public record DataValueUpdateMessage(@NotNull String dataAddress, String encodedValue) {
}
