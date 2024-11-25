package net.staticstudios.data.meta.persistant.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.value.ForeignPersistentValue;

import java.lang.reflect.Field;

public final class ForeignPersistentValueMetadata extends AbstractPersistentValueMetadata<ForeignPersistentValue<?>> {
    private final String linkingTable;
    private final String thisLinkingColumn;
    private final String foreignLinkingColumn;

    public ForeignPersistentValueMetadata(DataManager dataManager, String foreignTable, String linkingTable, String column, String thisLinkingColumn, String foreignLinkingColumn, Class<?> type, Field field, Class<? extends UniqueData> parentClass) {
        super(dataManager, foreignTable, column, type, field, parentClass);

        this.linkingTable = linkingTable;
        this.thisLinkingColumn = thisLinkingColumn;
        this.foreignLinkingColumn = foreignLinkingColumn;
    }

    /**
     * Extract the metadata from a {@link ForeignPersistentValue}.
     *
     * @param dataManager The data manager to use
     * @param parentClass The parent class that this collection is a member of
     * @param table       The table that the parent object uses
     * @param value       A dummy instance of the value
     * @return The metadata for the value
     */
    @SuppressWarnings("unused") //Used via reflection
    public static <T extends UniqueData> ForeignPersistentValueMetadata extract(DataManager dataManager, Class<T> parentClass, String table, ForeignPersistentValue<?> value, Field field) {
        return new ForeignPersistentValueMetadata(dataManager, value.getTable(), value.getLinkingTable(), value.getColumn(), value.getThisLinkingColumn(), value.getForeignLinkingColumn(), value.getType(), field, parentClass);
    }

    public String getLinkingTable() {
        return linkingTable;
    }

    public String getThisLinkingColumn() {
        return thisLinkingColumn;

    }

    public String getForeignLinkingColumn() {
        return foreignLinkingColumn;
    }
}
