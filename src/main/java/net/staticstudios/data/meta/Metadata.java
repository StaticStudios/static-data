package net.staticstudios.data.meta;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.shared.DataWrapper;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;

public interface Metadata {
    /**
     * Get the field that this metadata is associated with.
     *
     * @return the field that this metadata is associated with
     */
    Field getField();

    DataManager getDataManager();

    /**
     * Get the parent class of this metadata.
     *
     * @return The parent class
     */
    Class<? extends UniqueData> getParentClass();

    default DataWrapper getWrapper(UniqueData parent) {
        Field field = getField();
        try {
            return (DataWrapper) field.get(parent);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the address of this object. This address should be specific to what data is held in its respective {@link DataWrapper}.
     * This method is similar to {@link DataWrapper#getDataAddress()} however it does not factor in the parent's id, since there is no parent for metadata.
     *
     * @return The address
     */
    @NotNull String getMetadataAddress();
}
