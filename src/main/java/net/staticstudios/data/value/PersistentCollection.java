package net.staticstudios.data.value;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DatabaseSupportedType;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.PersistentCollectionChangeMessage;
import net.staticstudios.data.messaging.handle.PersistentCollectionAddMessageHandler;
import net.staticstudios.data.messaging.handle.PersistentCollectionRemoveMessageHandler;
import net.staticstudios.data.meta.persistant.collection.PersistentCollectionMetadata;
import net.staticstudios.data.meta.persistant.collection.PersistentEntryValueMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.SharedCollection;
import net.staticstudios.utils.Pair;
import net.staticstudios.utils.ThreadUtils;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * A collection of data that is stored in a database and is synced between multiple services.
 */
public class PersistentCollection<V extends CollectionEntry> extends SharedCollection<V, PersistentCollectionMetadata, Connection> {
    private final String table;
    private final String linkingColumn;
    private Consumer<V> addHandler;
    private Consumer<V> removeHandler;
    private Consumer<V> updateHandler;

    public PersistentCollection(UniqueData uniqueData, Class<V> type, String table, String linkingColumn) {
        super(uniqueData, type, PersistentCollection.class);
        this.table = table;
        this.linkingColumn = linkingColumn;
    }

    /**
     * Create a new PersistentCollection.
     *
     * @param data          The data object associated with this collection
     * @param type          The type of data being stored in the collection
     * @param table         The table where the data in the collection is stored
     * @param linkingColumn The column that links the data in the collection to the unique data. It will be the column that contains the unique data's id
     */
    public static <V extends CollectionEntry> PersistentCollection<V> of(UniqueData data, Class<V> type, String table, String linkingColumn) {
        return new PersistentCollection<>(data, type, table, linkingColumn);
    }

    /**
     * Set the handler that will be called when an entry is added to the collection.
     * Note that this handler is called on every server instance when the add update is received.
     * Also note that the handler is not guaranteed to run on a specific thread.
     *
     * @param addHandler The handler to call
     * @return The collection
     */
    public PersistentCollection<V> onAdd(Consumer<V> addHandler) {
        this.addHandler = addHandler;
        return this;
    }

    /**
     * Set the handler that will be called when an entry is removed from the collection.
     * Note that this handler is called on every server instance when the remove update is received.
     * Also note that the handler is not guaranteed to run on a specific thread.
     *
     * @param removeHandler The handler to call
     * @return The collection
     */
    public PersistentCollection<V> onRemove(Consumer<V> removeHandler) {
        this.removeHandler = removeHandler;
        return this;
    }

    /**
     * Set the handler that will be called when an entry is updated in the collection.
     * Note that this handler is called on every server instance when the update is received.
     * Also note that the handler is not guaranteed to run on a specific thread.
     *
     * @param updateHandler The handler to call
     */
    public PersistentCollection<V> onUpdate(Consumer<V> updateHandler) {
        this.updateHandler = updateHandler;
        return this;
    }

    @Override
    public Class<PersistentCollectionMetadata> getMetadataClass() {
        return PersistentCollectionMetadata.class;
    }

    @Override
    protected void addAllToDataSource(Collection<V> values) {
        DataManager dataManager = getMetadata().getDataManager();
        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                addAllToDataSource(connection, values, true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected void removeAllFromDataSource(Collection<V> values) {
        DataManager dataManager = getMetadata().getDataManager();
        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                removeAllFromDataSource(connection, values, true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    @Blocking
    public void removeAllFromDataSource(Connection connection, Collection<V> values, boolean sendMessage) {
        DataManager dataManager = getMetadata().getDataManager();
        try {
            PreparedStatement statement = null;

            if (values.isEmpty()) {
                return;
            }

            for (V entry : values) {
                List<ColumnMapping> columnMappings = new ArrayList<>();
                columnMappings.add(ColumnMapping.of(linkingColumn, getData().getId(), true, false));
                columnMappings.addAll(getColumnMappings(entry, true));

                if (statement == null) {
                    StringBuilder builder = new StringBuilder("DELETE FROM ");
                    builder.append(table);

                    builder.append(" WHERE ctid IN (SELECT ctid FROM ");
                    builder.append(table);
                    builder.append(" WHERE ");

                    for (int i = 0; i < columnMappings.size(); i++) {
                        ColumnMapping columnMapping = columnMappings.get(i);
                        builder.append(columnMapping.column());
                        builder.append(" = ?");
                        if (i < columnMappings.size() - 1) {
                            builder.append(" AND ");
                        }
                    }

                    builder.append(" LIMIT 1 )");
                    String sql = builder.toString();

                    DataManager.debug("PersistentCollection removeAllFromDataSource statement: " + sql);

                    statement = connection.prepareStatement(sql);
                }

                for (int i = 0; i < columnMappings.size(); i++) {
                    ColumnMapping columnMapping = columnMappings.get(i);
                    statement.setObject(i + 1, dataManager.serialize(columnMapping.value()));
                }

                statement.addBatch();
            }

            statement.executeBatch();

            List<Map<String, String>> pkeyValues = new ArrayList<>();
            for (V entry : values) {
                Map<String, String> valueMap = new HashMap<>();
                for (ColumnMapping columnMapping : getColumnMappings(entry, true)) {
                    Object serialized = dataManager.serialize(columnMapping.value());
                    String encoded = DatabaseSupportedType.encode(serialized);
                    valueMap.put(columnMapping.column(), encoded);
                }
                pkeyValues.add(valueMap);
            }

            if (sendMessage) {
                dataManager.getMessenger().broadcastMessageNoPrefix(
                        dataManager.getDataChannel(this),
                        PersistentCollectionRemoveMessageHandler.class,
                        new PersistentCollectionChangeMessage(
                                getDataAddress(),
                                pkeyValues
                        ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateInDataSource(CollectionEntry entry) {
        DataManager dataManager = getMetadata().getDataManager();
        ThreadUtils.submit(() -> {
            try (Connection connection = dataManager.getConnection()) {
                updateInDataSource(connection, entry);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void updateInDataSource(Connection connection, CollectionEntry entry) {
        DataManager dataManager = getMetadata().getDataManager();
        try {
            List<ColumnMapping> columnMappings = new ArrayList<>(getColumnMappings(entry, false));

            columnMappings.removeIf(columnMapping -> !columnMapping.mutable());

            StringBuilder builder = new StringBuilder("UPDATE ");
            builder.append(table);
            builder.append(" SET ");

            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping columnMapping = columnMappings.get(i);
                builder.append(columnMapping.column());
                builder.append(" = ?");
                if (i < columnMappings.size() - 1) {
                    builder.append(", ");
                }
            }

            builder.append(" WHERE ctid IN (SELECT ctid FROM ");
            builder.append(table);
            builder.append(" WHERE ");

            List<Pair<String, Object>> whereColumns = new ArrayList<>();
            for (PersistentEntryValueMetadata valueMetadata : getMetadata().getEntryValueMetadata()) {
                if (valueMetadata.isPkey()) {
                    whereColumns.add(Pair.of(valueMetadata.getColumn(), valueMetadata.getValue(entry).getInternalValue()));
                }
            }
            whereColumns.add(Pair.of(linkingColumn, getData().getId()));
            for (int i = 0; i < whereColumns.size(); i++) {
                String column = whereColumns.get(i).first();
                builder.append(column);
                builder.append(" = ?");
                if (i < whereColumns.size() - 1) {
                    builder.append(" AND ");
                }
            }

            builder.append(" LIMIT 1 )");
            String sql = builder.toString();

            DataManager.debug("PersistentCollection updateInDataSource statement: " + sql);

            PreparedStatement statement = connection.prepareStatement(sql);

            for (int i = 0; i < columnMappings.size(); i++) {
                ColumnMapping columnMapping = columnMappings.get(i);

                PersistentEntryValue<?> entryValue = null;
                for (PersistentEntryValueMetadata valueMetadata : getMetadata().getEntryValueMetadata()) {
                    if (valueMetadata.getColumn().equals(columnMapping.column())) {
                        entryValue = valueMetadata.getValue(entry);
                        break;
                    }
                }

                if (entryValue == null) {
                    throw new RuntimeException("Failed to find entry value for column: " + columnMapping.column());
                }

                statement.setObject(i + 1, dataManager.serialize(entryValue.getInternalValue()));
            }

            for (int i = 0; i < whereColumns.size(); i++) {
                Object value = whereColumns.get(i).second();
                statement.setObject(columnMappings.size() + i + 1, dataManager.serialize(value));
            }

            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public @NotNull Consumer<V> getAddHandler() {
        if (addHandler == null) {
            return v -> {
            };
        }
        return addHandler;
    }

    @Override
    public @NotNull Consumer<V> getRemoveHandler() {
        if (removeHandler == null) {
            return v -> {
            };
        }
        return removeHandler;
    }

    @Override
    public @NotNull Consumer<V> getUpdateHandler() {
        if (updateHandler == null) {
            return v -> {
            };
        }
        return updateHandler;
    }

    @Override
    @Blocking
    public void addAllToDataSource(Connection connection, Collection<V> values, boolean sendMessage) {
        DataManager dataManager = getMetadata().getDataManager();
        try {
            PreparedStatement statement = null;

            if (values.isEmpty()) {
                return;
            }

            for (V entry : values) {
                List<ColumnMapping> columnMappings = new ArrayList<>();
                columnMappings.add(ColumnMapping.of(linkingColumn, getData().getId(), true, false));
                columnMappings.addAll(getColumnMappings(entry, false));

                if (statement == null) {
                    StringBuilder builder = new StringBuilder("INSERT INTO ");
                    builder.append(table);
                    builder.append(" (");

                    for (int i = 0; i < columnMappings.size(); i++) {
                        ColumnMapping columnMapping = columnMappings.get(i);
                        builder.append(columnMapping.column());
                        if (i < columnMappings.size() - 1) {
                            builder.append(", ");
                        }
                    }

                    builder.append(") VALUES (");

                    for (int i = 0; i < columnMappings.size(); i++) {
                        builder.append("?");
                        if (i < columnMappings.size() - 1) {
                            builder.append(", ");
                        }
                    }

                    builder.append(")");
                    String sql = builder.toString();

                    DataManager.debug("PersistentCollection addAllToDataSource statement: " + sql);

                    statement = connection.prepareStatement(sql);
                }

                for (int i = 0; i < columnMappings.size(); i++) {
                    ColumnMapping columnMapping = columnMappings.get(i);
                    statement.setObject(i + 1, dataManager.serialize(columnMapping.value()));
                }

                statement.addBatch();
            }

            statement.executeBatch();

            List<Map<String, String>> changedValues = new ArrayList<>();
            for (V entry : values) {
                Map<String, String> valueMap = new HashMap<>();
                for (ColumnMapping columnMapping : getColumnMappings(entry, false)) {
                    Object serialized = dataManager.serialize(columnMapping.value());
                    String encoded = DatabaseSupportedType.encode(serialized);
                    valueMap.put(columnMapping.column(), encoded);
                }
                changedValues.add(valueMap);
            }

            if (sendMessage) {
                dataManager.getMessenger().broadcastMessageNoPrefix(
                        dataManager.getDataChannel(this),
                        PersistentCollectionAddMessageHandler.class,
                        new PersistentCollectionChangeMessage(
                                getDataAddress(),
                                changedValues
                        ));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Blocking
    public void addInternalValuesToDataSource(Connection connection) {
        addAllToDataSource(connection, this, false);
    }

    /**
     * Get the table where the data in the collection is stored
     *
     * @return The table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Get the column that links the data in the collection to the unique data.
     * It will be the column that contains the unique data's id
     *
     * @return The linking column
     */
    public String getLinkingColumn() {
        return linkingColumn;
    }

    @Blocking
    public void add(Connection connection, V v) {
        addAll(connection, Collections.singletonList(v));
    }

    @Blocking
    public void remove(Connection connection, V v) {
        removeAll(connection, Collections.singletonList(v));
    }

    @Override
    public String getDataAddress() {
        return getData().getId() + ".collection." + table + "." + linkingColumn;
    }

    @Blocking
    public synchronized void removeAll(Connection connection, @NotNull Collection<?> c) {
        List<V> values = new ArrayList<>();

        for (Object o : c) {
            if (o instanceof CollectionEntry) {
                values.add((V) o);
            }
        }

        removeAllInternal(values);

        removeAllFromDataSource(connection, values, true);
    }

    @Blocking
    public synchronized void addAll(Connection connection, @NotNull Collection<? extends V> c) {
        List<V> values = new ArrayList<>(c);
        addAllInternal(values, true);

        addAllToDataSource(connection, values, true);
    }

    private List<ColumnMapping> getColumnMappings(CollectionEntry entry, boolean onlyPkeys) {
        List<ColumnMapping> columnMappings = new ArrayList<>();

        PersistentCollectionMetadata collectionMetadata = getMetadata();
        List<PersistentEntryValueMetadata> entryValueMetadata = collectionMetadata.getEntryValueMetadata();

        for (PersistentEntryValueMetadata valueMetadata : entryValueMetadata) {
            PersistentEntryValue<?> value = valueMetadata.getValue(entry);
            columnMappings.add(ColumnMapping.of(valueMetadata.getColumn(), value.getSyncedValue(), valueMetadata.isPkey(), value.isMutable()));
        }

        if (onlyPkeys) {
            columnMappings.removeIf(columnMapping -> !columnMapping.pkey);
        }

        return columnMappings;
    }

    record ColumnMapping(String column, Object value, boolean pkey, boolean mutable) {
        public static ColumnMapping of(String column, Object value, boolean pkey, boolean mutable) {
            return new ColumnMapping(column, value, pkey, mutable);
        }
    }
}
