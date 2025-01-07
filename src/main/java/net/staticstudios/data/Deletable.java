package net.staticstudios.data;

import org.jetbrains.annotations.NotNull;

public interface Deletable {
    Deletable deletionStrategy(DeletionStrategy strategy);

    @NotNull DeletionStrategy getDeletionStrategy();
}
