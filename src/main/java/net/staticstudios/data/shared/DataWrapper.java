package net.staticstudios.data.shared;

import org.jetbrains.annotations.Nullable;

public interface DataWrapper {

    /**
     * Get the address of this object, which includes the id of the parent
     *
     * @return The address
     */
    @Nullable String getDataAddress();
}
