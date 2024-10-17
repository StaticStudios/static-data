package net.staticstudios.data.meta;

import net.staticstudios.data.Addressable;
import net.staticstudios.data.DataManager;

import java.lang.reflect.Field;

public interface Metadata extends Addressable {
    /**
     * Get the field that this metadata is associated with.
     *
     * @return the field that this metadata is associated with
     */
    Field getField();

    DataManager getDataManager();
}
