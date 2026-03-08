package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

public interface PersistentCollectionMetadata {
    Class<? extends UniqueData> getHolderClass();

    boolean hasValidatedChangeHandlers();

    void setValidatedChangeHandlers(boolean validatedChangeHandlers);
}
