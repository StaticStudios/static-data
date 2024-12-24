package net.staticstudios.data.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.CollectionEntry;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.collection.PersistentValueCollection;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.UniqueIdentifier;
import org.jetbrains.annotations.Blocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class PersistentCollectionManager {
    private static PersistentCollectionManager instance;
    private final Logger logger = LoggerFactory.getLogger(PersistentCollectionManager.class);
    private final DataManager dataManager;
    private final PostgresListener pgListener;
    /**
     * Maps a collection key to a list of data keys for each entry in the collection
     */
    private final Multimap<CollectionKey, UniqueIdentifier> collectionEntryHolders;

    protected PersistentCollectionManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;
        this.collectionEntryHolders = Multimaps.synchronizedSetMultimap(HashMultimap.create());


        pgListener.addHandler(notification -> {
            // The PersistentValueManager will handle updating the main cache
            // All we have to concern ourselves with is updating the collection entry holders
            // Note about updates: we have to care about when the linking column changes, since that's what we use to identify the holder

            Collection<PersistentCollection<?>> dummyCollections = dataManager.getDummyPersistentCollections(notification.getSchema() + "." + notification.getTable());
            switch (notification.getOperation()) {
                case INSERT -> dummyCollections.forEach(dummyCollection -> {
                    String encodedLinkingId = notification.getData().newDataValueMap().get(dummyCollection.getLinkingColumn());
                    if (encodedLinkingId == null) {
                        return;
                    }

                    CollectionKey collectionKey = new CollectionKey(
                            dummyCollection.getSchema(),
                            dummyCollection.getTable(),
                            dummyCollection.getLinkingColumn(),
                            dummyCollection.getDataColumn(),
                            UUID.fromString(encodedLinkingId)
                    );

                    collectionEntryHolders.put(collectionKey, UniqueIdentifier.of(
                            dummyCollection.getRootHolder().getIdentifier().getColumn(),
                            UUID.fromString(notification.getData().newDataValueMap().get(dummyCollection.getEntryIdColumn()))
                    ));
                });
                case UPDATE -> dummyCollections.forEach(dummyCollection -> {
                    String linkingColumn = dummyCollection.getLinkingColumn();

                    String oldLinkingValue = notification.getData().oldDataValueMap().get(linkingColumn);
                    String newLinkingValue = notification.getData().newDataValueMap().get(linkingColumn);

                    if (Objects.equals(oldLinkingValue, newLinkingValue)) {
                        return;
                    }


                    UUID entryId = UUID.fromString(notification.getData().newDataValueMap().get(dummyCollection.getEntryIdColumn()));


                    UUID oldLinkingId = oldLinkingValue == null ? null : UUID.fromString(oldLinkingValue);
                    UUID newLinkingId = newLinkingValue == null ? null : UUID.fromString(newLinkingValue);
                    if (oldLinkingId != null) {
                        CollectionKey collectionKey = new CollectionKey(
                                dummyCollection.getSchema(),
                                dummyCollection.getTable(),
                                linkingColumn,
                                dummyCollection.getDataColumn(),
                                UUID.fromString(oldLinkingValue)
                        );

                        UniqueIdentifier oldIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), entryId);
                        logger.trace("Removed collection entry holder from map: {} -> {}", collectionKey, oldIdentifier);
                    }

                    if (newLinkingId != null) {
                        CollectionKey collectionKey = new CollectionKey(
                                dummyCollection.getSchema(),
                                dummyCollection.getTable(),
                                linkingColumn,
                                dummyCollection.getDataColumn(),
                                UUID.fromString(newLinkingValue)
                        );

                        UniqueIdentifier newIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), entryId);
                        collectionEntryHolders.put(collectionKey, newIdentifier);
                        logger.trace("Added collection entry holder to map: {} -> {}", collectionKey, newIdentifier);
                    }

                });
                case DELETE -> dummyCollections.forEach(dummyCollection -> {
                    String encodedLinkingId = notification.getData().oldDataValueMap().get(dummyCollection.getLinkingColumn());
                    if (encodedLinkingId == null) {
                        return;
                    }

                    CollectionKey collectionKey = new CollectionKey(
                            dummyCollection.getSchema(),
                            dummyCollection.getTable(),
                            dummyCollection.getLinkingColumn(),
                            dummyCollection.getDataColumn(),
                            UUID.fromString(encodedLinkingId)
                    );

                    collectionEntryHolders.remove(collectionKey, UniqueIdentifier.of(
                            dummyCollection.getRootHolder().getIdentifier().getColumn(),
                            UUID.fromString(notification.getData().oldDataValueMap().get(dummyCollection.getEntryIdColumn()))
                    ));
                });
            }
        });
    }

    public static void instantiate(DataManager dataManager, PostgresListener pgListener) {
        instance = new PersistentCollectionManager(dataManager, pgListener);
    }

    public static PersistentCollectionManager getInstance() {
        return instance;
    }

    public void handlePersistentValueUncache(String schema, String table, String column, UUID holderId, String idColumn, Object oldValue) {
        // This will look at the linking column, and if it's been removed, then we need to remove the entry from the collection
        if (oldValue == null || !oldValue.getClass().equals(UUID.class)) {
            return;
        }

        logger.trace("Handling PersistentValue uncache: ({}.{}.{}) {}", schema, table, column, oldValue);
        UUID oldEntryId = (UUID) oldValue;


        dataManager.getDummyPersistentCollections(schema + "." + table).stream()
                .filter(dummyCollection -> dummyCollection.getEntryIdColumn().equals(column))
                .forEach(dummyCollection -> {

                    UniqueIdentifier oldIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), oldEntryId);
                    CellKey linkingKey = getEntryLinkingKey(dummyCollection, oldEntryId, oldIdentifier);

                    CollectionKey oldCollectionKey = new CollectionKey(
                            schema,
                            table,
                            dummyCollection.getLinkingColumn(),
                            dummyCollection.getDataColumn(),
                            dataManager.get(linkingKey)
                    );

                    collectionEntryHolders.remove(oldCollectionKey, oldIdentifier);
                    logger.trace("Removed collection entry holder from map: {} -> {}", oldCollectionKey, oldIdentifier);
                });
    }

    public void handlePersistentValueCacheUpdated(String schema, String table, String column, UUID holderId, String idColumn, Object oldValue, Object newValue) {
        if (Objects.equals(oldValue, newValue)) {
            return;
        }
        if (oldValue != null && !oldValue.getClass().equals(UUID.class)) {
            return;
        }

        if (newValue != null && !newValue.getClass().equals(UUID.class)) {
            return;
        }

        logger.trace("Handling PersistentValue cache update: ({}.{}.{}) {} -> {}", schema, table, column, oldValue, newValue);

        UUID newLinkingId = (UUID) newValue;
        UUID oldLinkingId = (UUID) oldValue;

        // We need to check if a collection exists for this pv and this pv is the linking column, if so then we need to update the collection entry holder
        // This will add or remove the entry from the collection

        dataManager.getDummyPersistentCollections(schema + "." + table).stream()
                .filter(dummyCollection -> dummyCollection.getLinkingColumn().equals(column))
                .forEach(dummyCollection -> {

                    CellKey entryIdKey = new CellKey(
                            schema,
                            table,
                            dummyCollection.getEntryIdColumn(),
                            holderId,
                            idColumn
                    );

                    if (oldLinkingId != null) {
                        CollectionKey oldCollectionKey = new CollectionKey(
                                dummyCollection.getSchema(),
                                dummyCollection.getTable(),
                                dummyCollection.getLinkingColumn(),
                                dummyCollection.getDataColumn(),
                                oldLinkingId
                        );

                        UUID oldEntryId = dataManager.get(entryIdKey);

                        UniqueIdentifier oldIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), oldEntryId);
                        collectionEntryHolders.remove(oldCollectionKey, oldIdentifier);
                        logger.trace("Removed collection entry holder from map: {} -> {}", dummyCollection.getKey(), oldIdentifier);
                    }

                    if (newLinkingId != null) {
                        CollectionKey newCollectionKey = new CollectionKey(
                                dummyCollection.getSchema(),
                                dummyCollection.getTable(),
                                dummyCollection.getLinkingColumn(),
                                dummyCollection.getDataColumn(),
                                newLinkingId
                        );

                        UUID newEntryId = dataManager.get(entryIdKey);

                        UniqueIdentifier newIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), newEntryId);
                        collectionEntryHolders.put(newCollectionKey, newIdentifier);
                        logger.trace("Added collection entry holder to map: {} -> {}", newCollectionKey, newIdentifier);
                    }
                });
    }

    @Blocking
    @SuppressWarnings("unchecked")
    public void loadAllFromDatabase(Connection connection, UniqueData dummyHolder, PersistentCollection<?> dummyCollection) throws SQLException {
        String schemaTable = dummyCollection.getSchema() + "." + dummyCollection.getTable();
        pgListener.ensureTableHasTrigger(connection, schemaTable);

        List<UniqueData> holders = dataManager.getAll((Class<UniqueData>) dummyHolder.getClass());

        // Load our map so we know what entries belong to which holders
        for (UniqueData holder : holders) {
            CollectionKey collectionKey = new CollectionKey(
                    dummyCollection.getSchema(),
                    dummyCollection.getTable(),
                    dummyCollection.getLinkingColumn(),
                    holder.getIdentifier().getColumn(),
                    holder.getId()
            );
            UniqueIdentifier holderIdentifier = holder.getIdentifier();

            collectionEntryHolders.put(collectionKey, holderIdentifier);
        }

        String entryIdColumn = dummyCollection.getEntryIdColumn();
        String entryDataColumn = dummyCollection.getDataColumn();
        String collectionLinkingColumn = dummyCollection.getLinkingColumn();

        String sql = "SELECT " + entryIdColumn + ", " + collectionLinkingColumn;

        if (!entryIdColumn.equals(entryDataColumn)) {
            // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
            sql += ", " + entryDataColumn;
        }

        sql += " FROM " + dummyCollection.getSchema() + "." + dummyCollection.getTable();

        dataManager.logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                UUID entryId = (UUID) resultSet.getObject(entryIdColumn);
                UUID linkingId = (UUID) resultSet.getObject(collectionLinkingColumn);

                UniqueIdentifier uniqueIdentifier = UniqueIdentifier.of(dummyHolder.getIdentifier().getColumn(), entryId);
                CellKey entryDataKey = getEntryDataKey(dummyCollection, entryId, uniqueIdentifier);
                CellKey entryLinkKey = getEntryLinkingKey(dummyCollection, entryId, uniqueIdentifier);

                // I ordered the logs like this so that the output is in the same order as it is elsewhere in this class

                logger.trace("Adding collection entry to {}", dummyCollection.getKey());
                logger.trace("Adding collection entry link to cache: {} -> {}", entryLinkKey, linkingId);

                if (!entryIdColumn.equals(entryDataColumn)) {
                    // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
                    Object serializedDataValue = resultSet.getObject(entryDataColumn);
                    Object dataValue = dataManager.deserialize(dummyCollection.getDataType(), serializedDataValue);
                    dataManager.cache(entryDataKey, dummyCollection.getDataType(), dataValue, Instant.now());

                    logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, dataValue);
                }

                logger.trace("Adding collection entry holder to map: {} -> {}", dummyCollection.getKey(), uniqueIdentifier);

                dataManager.cache(entryLinkKey, UUID.class, linkingId, Instant.now());
                collectionEntryHolders.put(dummyCollection.getKey(), uniqueIdentifier);
            }
        }
    }

    public void addEntriesToCache(PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) {
        UniqueData holder = collection.getRootHolder();

        for (CollectionEntry entry : entries) {
            UniqueIdentifier uniqueIdentifier = UniqueIdentifier.of(holder.getIdentifier().getColumn(), entry.id());
            CellKey entryDataKey = getEntryDataKey(collection, entry.id(), holder.getIdentifier());
            CellKey entryLinkKey = getEntryLinkingKey(collection, entry.id(), holder.getIdentifier());

            logger.trace("Adding collection entry to {}", collection.getKey());
            logger.trace("Adding collection entry link to cache: {} -> {}", entryLinkKey, collection.getRootHolder().getId());
            logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, entry.value());
            logger.trace("Adding collection entry holder to map: {} -> {}", collection.getKey(), uniqueIdentifier);

            dataManager.cache(entryLinkKey, UUID.class, collection.getRootHolder().getId(), Instant.now());
            dataManager.cache(entryDataKey, collection.getDataType(), entry.value(), Instant.now());
            collectionEntryHolders.put(collection.getKey(), uniqueIdentifier);
        }
    }

    public void removeEntriesFromCache(PersistentValueCollection<?> collection, List<CollectionEntry> entries) {
        DataKey collectionKey = collection.getKey();
        for (CollectionEntry entry : entries) {
            collectionEntryHolders.remove(collectionKey, UniqueIdentifier.of(collection.getRootHolder().getIdentifier().getColumn(), entry.id()));
            dataManager.uncache(getEntryDataKey(collection, entry.id(), collection.getRootHolder().getIdentifier()));
        }
    }


    public CellKey getEntryDataKey(PersistentCollection<?> collection, UUID entryId, UniqueIdentifier holderIdentifier) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getDataColumn(), entryId, holderIdentifier.getColumn());
    }

    public CellKey getEntryLinkingKey(PersistentCollection<?> collection, UUID entryId, UniqueIdentifier holderIdentifier) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), entryId, holderIdentifier.getColumn());
    }

    public List<CellKey> getEntryKeys(PersistentCollection<?> collection) {
        return collectionEntryHolders.get(collection.getKey()).stream().map(k -> getEntryDataKey(collection, k.getId(), collection.getRootHolder().getIdentifier())).toList();
    }

    public List<Object> getEntries(PersistentCollection<?> collection) {
        return getEntryKeys(collection).stream().map(dataManager::get).toList();
    }

    public List<CollectionEntry> getCollectionEntries(PersistentCollection<?> collection) {
        return collectionEntryHolders.get(collection.getKey()).stream().map(identifier -> {
            CellKey entryDataKey = getEntryDataKey(collection, identifier.getId(), collection.getRootHolder().getIdentifier());
            Object value = dataManager.get(entryDataKey);
            return new CollectionEntry(identifier.getId(), value);
        }).toList();
    }

    public void addToDatabase(Connection connection, PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) throws SQLException {
        if (entries.isEmpty()) {
            return;
        }

        String entryIdColumn = collection.getEntryIdColumn();

        //todo: this insert thing is wrong, since it will fail if there is a not null constraint on some other column not specified
        //todo: change this to a function or something where we try to update an existing entry, if its not there, then we fallback to insert. this case is when were using a uniquedatacollection
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" (");

        List<String> columns = new ArrayList<>();

        columns.add(entryIdColumn);
        if (!collection.getDataColumn().equals(entryIdColumn)) {
            // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
            columns.add(collection.getDataColumn());
        }
        columns.add(collection.getLinkingColumn());

        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append(columns.get(i));
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(") VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append("?");
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(")");

        sqlBuilder.append(" ON CONFLICT (");
        sqlBuilder.append(entryIdColumn);
        sqlBuilder.append(") DO UPDATE SET ");
        sqlBuilder.append(collection.getLinkingColumn()).append(" = EXCLUDED.").append(collection.getLinkingColumn());

        String sql = sqlBuilder.toString();
        dataManager.logSQL(sql);


        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {

            for (CollectionEntry entry : entries) {

                int i = 1;
                statement.setObject(i, entry.id());
                if (!collection.getDataColumn().equals(entryIdColumn)) {
                    // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
                    statement.setObject(++i, dataManager.serialize(entry.value()));
                }
                statement.setObject(++i, collection.getRootHolder().getId());

                statement.executeUpdate();
            }

        } finally {
            connection.setAutoCommit(autoCommit);
        }

    }


    public void removeFromDatabase(Connection connection, PersistentValueCollection<?> collection, List<CollectionEntry> entries) throws SQLException {
        if (entries.isEmpty()) {
            return;
        }

        UniqueIdentifier firstPkey = collection.getRootHolder().getIdentifier();
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" WHERE (");

        sqlBuilder.append(firstPkey.getColumn());

        sqlBuilder.append(") IN (");

        int totalValues = entries.size();

        for (int i = 0; i < totalValues; i++) {
            sqlBuilder.append("?");
            if (i < totalValues - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(")");


        String sql = sqlBuilder.toString();
        dataManager.logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;

            for (CollectionEntry entry : entries) {
                statement.setObject(i++, entry.id());
            }

            statement.executeUpdate();
        }
    }

    public void removeFromUniqueDataCollectionInMemory(PersistentValueCollection<?> idsCollection, List<UUID> entryIds) {
        if (entryIds.isEmpty()) {
            return;
        }

        for (UUID entry : entryIds) {
            UniqueIdentifier pkey = UniqueIdentifier.of(idsCollection.getRootHolder().getIdentifier().getColumn(), entry);
            collectionEntryHolders.remove(idsCollection.getKey(), pkey);
        }
    }

    public void removeFromUniqueDataCollectionInDatabase(Connection connection, PersistentValueCollection<?> idsCollection, List<UUID> entryIds) throws SQLException {
        if (entryIds.isEmpty()) {
            return;
        }

        StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(idsCollection.getSchema()).append(".").append(idsCollection.getTable()).append(" SET ");
        sqlBuilder.append(idsCollection.getLinkingColumn()).append(" = NULL WHERE ");
        sqlBuilder.append(idsCollection.getEntryIdColumn()).append(" IN (");

        int totalValues = entryIds.size();

        for (int i = 0; i < totalValues; i++) {
            sqlBuilder.append("?");
            if (i < totalValues - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();
        dataManager.logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;

            for (UUID entry : entryIds) {
                statement.setObject(i++, entry);
            }

            statement.executeUpdate();
        }
    }
}
