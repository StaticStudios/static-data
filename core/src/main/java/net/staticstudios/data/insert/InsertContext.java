package net.staticstudios.data.insert;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;
import net.staticstudios.data.InsertStrategy;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.SimpleColumnMetadata;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class InsertContext {
    private final AtomicBoolean inserted = new AtomicBoolean(false);
    private final DataManager dataManager;
    private final Map<SimpleColumnMetadata, Object> entries = new HashMap<>();
    private final Map<SimpleColumnMetadata, InsertStrategy> insertStrategies = new HashMap<>();

    public InsertContext(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public InsertContext set(String schema, String table, String column, @Nullable Object value, @Nullable InsertStrategy insertStrategy) {
        Preconditions.checkState(!inserted.get(), "Cannot modify InsertContext after it has been inserted");
        SQLSchema sqlSchema = dataManager.getSQLBuilder().getSchema(schema);
        Preconditions.checkNotNull(sqlSchema, "Schema not found: " + schema);
        SQLTable sqlTable = sqlSchema.getTable(table);
        Preconditions.checkNotNull(sqlTable, "Table not found: " + table);
        SQLColumn sqlColumn = sqlTable.getColumn(column);
        Preconditions.checkNotNull(sqlColumn, "Column not found: " + column + " in table: " + table + " schema: " + schema);

        SimpleColumnMetadata columnMetadata = new SimpleColumnMetadata(
                schema,
                table,
                column,
                sqlColumn.getType()
        );

        if (value == null) {
            entries.remove(columnMetadata);
            insertStrategies.remove(columnMetadata);
            return this;
        }

        if (insertStrategy != null) {
            insertStrategies.put(columnMetadata, insertStrategy);
        }

        Preconditions.checkArgument(sqlColumn.getType().isAssignableFrom(dataManager.getSerializedType(value.getClass())), "Value type mismatch for name " + column + " in table " + table + " schema " + schema + ". Expected: " + sqlColumn.getType().getName() + ", got: " + Objects.requireNonNull(value).getClass().getName());

        entries.put(columnMetadata, dataManager.serialize(value));
        return this;
    }

    public Map<SimpleColumnMetadata, Object> getEntries() {
        return entries;
    }

    public Map<SimpleColumnMetadata, InsertStrategy> getInsertStrategies() {
        return insertStrategies;
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
        boolean insertedAllIdColumns = metadata.idColumns().stream()
                .allMatch(idColumn -> entries.keySet().stream()
                        .anyMatch(entry -> Objects.equals(entry.schema(), idColumn.schema()) &&
                                Objects.equals(entry.table(), idColumn.table()) &&
                                Objects.equals(entry.name(), idColumn.name())));

        Preconditions.checkState(insertedAllIdColumns, "The requested class was not inserted. Class: " + holderClass.getName() + " is missing one or more ID name values. Required ID columns: " + metadata.idColumns());
        ColumnValuePair[] idColumnValues = new ColumnValuePair[metadata.idColumns().size()];
        for (int i = 0; i < metadata.idColumns().size(); i++) {
            idColumnValues[i] = new ColumnValuePair(metadata.idColumns().get(i).name(), entries.get(new SimpleColumnMetadata(metadata.idColumns().get(i))));
        }
        return dataManager.getInstance(holderClass, idColumnValues);
    }
}
