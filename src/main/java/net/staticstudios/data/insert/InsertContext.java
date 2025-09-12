package net.staticstudios.data.insert;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.parse.UniqueDataMetadata;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.ColumnValuePair;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class InsertContext { //todo: insert strategy, on a per pv level.
    private final AtomicBoolean inserted = new AtomicBoolean(false);
    private final DataManager dataManager;
    private final Map<ColumnMetadata, Object> entries = new HashMap<>();

    public InsertContext(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public <T extends UniqueData> InsertContext set(Class<T> holderClass, String column, Object value) {
        UniqueDataMetadata metadata = dataManager.getMetadata(holderClass);
        Preconditions.checkNotNull(metadata, "Metadata not found for class: " + holderClass.getName());
        set(metadata.schema(), metadata.table(), column, value);
        return this;
    }

    public InsertContext set(String schema, String table, String column, @Nullable Object value) {
        if (value == null) {
            return this; //todo: realistically we should validate the nullability stuff when we actually insert for better consistency.
        }
        Preconditions.checkState(!inserted.get(), "Cannot modify InsertContext after it has been inserted");
        SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(schema);
        Preconditions.checkNotNull(sqlSchema, "Schema not found: " + schema);
        SQLTable sqlTable = sqlSchema.getTable(table);
        Preconditions.checkNotNull(sqlTable, "Table not found: " + table);
        SQLColumn sqlColumn = sqlTable.getColumn(column);
        Preconditions.checkNotNull(sqlColumn, "Column not found: " + column + " in table: " + table + " schema: " + schema);
        Preconditions.checkArgument(value != null || sqlColumn.isNullable(), "Column " + column + " in table " + table + " schema " + schema + " cannot be null");
        Preconditions.checkArgument(sqlColumn.getType().isInstance(value), "Value type mismatch for column " + column + " in table " + table + " schema " + schema + ". Expected: " + sqlColumn.getType().getName() + ", got: " + Objects.requireNonNull(value).getClass().getName());

        ColumnMetadata columnMetadata = new ColumnMetadata(column, sqlColumn.getType(), sqlColumn.isNullable(), sqlColumn.isIndexed(), table, schema);
        entries.put(columnMetadata, value);
        return this;
    }

    public Map<ColumnMetadata, Object> getEntries() {
        return entries;
    }

    public void markInserted() {
        inserted.set(true);
    }

    public InsertContext insert(InsertMode insertMode) {
        dataManager.insert(this, insertMode);
        return this;
    }

    /**
     * Retrieves an instance of the specified UniqueData class based on the ID columns set in this InsertContext.
     *
     * @param holderClass The class of the UniqueData to retrieve.
     * @param <T>         The type of UniqueData.
     * @return An instance of the specified UniqueData class.
     */
    public <T extends UniqueData> T get(Class<T> holderClass) {
        UniqueDataMetadata metadata = dataManager.getMetadata(holderClass);
        Preconditions.checkNotNull(metadata, "Metadata not found for class: " + holderClass.getName());
        SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(metadata.schema());
        Preconditions.checkNotNull(sqlSchema, "Schema not found: " + metadata.schema());
        SQLTable sqlTable = sqlSchema.getTable(metadata.table());
        Preconditions.checkNotNull(sqlTable, "Table not found: " + metadata.table());
        boolean insertedAllIdColumns = true;
        for (ColumnMetadata idColumn : metadata.idColumns()) {
            if (!entries.containsKey(idColumn)) {
                insertedAllIdColumns = false;
                break;
            }
        }

        Preconditions.checkState(insertedAllIdColumns, "The requested class was not inserted. Class: " + holderClass.getName() + " is missing one or more ID column values. Required ID columns: " + metadata.idColumns());
        ColumnValuePair[] idColumnValues = new ColumnValuePair[metadata.idColumns().size()];
        for (int i = 0; i < metadata.idColumns().size(); i++) {
            idColumnValues[i] = new ColumnValuePair(metadata.idColumns().get(i).name(), entries.get(metadata.idColumns().get(i)));
        }
        return dataManager.get(holderClass, idColumnValues);
    }
}
