package net.staticstudios.data.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.UniqueData;
import net.staticstudios.data.data.collection.CollectionEntry;
import net.staticstudios.data.data.collection.KeyedCollectionEntry;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.data.collection.PersistentValueCollection;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.UniqueIdentifier;
import net.staticstudios.utils.ThreadUtils;
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
        this.collectionEntryHolders = Multimaps.synchronizedMultimap(ArrayListMultimap.create());


        pgListener.addHandler(notification -> {
            // The PersistentValueManager will handle updating the main cache
            // All we have to concern ourselves with is updating the collection entry holders
            // Note about updates: we have to care about when the linking column changes, since that's what we use to identify the holder

            Collection<PersistentCollection<?>> dummyCollections = dataManager.getDummyPersistentCollections(notification.getSchema() + "." + notification.getTable());
            switch (notification.getOperation()) {
                case INSERT -> dummyCollections.forEach(dummyCollection ->
                        collectionEntryHolders.put(dummyCollection.getKey(), dummyCollection.getRootHolder().getIdentifier()));
                case UPDATE -> dummyCollections.forEach(dummyCollection -> {
                    String linkingColumn = dummyCollection.getLinkingColumn();

                    String oldLinkingValue = notification.getData().oldDataValueMap().get(linkingColumn);
                    String newLinkingValue = notification.getData().newDataValueMap().get(linkingColumn);

                    if (Objects.equals(oldLinkingValue, newLinkingValue)) {
                        return;
                    }

                    UUID oldLinkingId = oldLinkingValue == null ? null : UUID.fromString(oldLinkingValue);
                    UUID newLinkingId = newLinkingValue == null ? null : UUID.fromString(newLinkingValue);

                    UniqueIdentifier oldIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), oldLinkingId);
                    UniqueIdentifier newIdentifier = UniqueIdentifier.of(dummyCollection.getRootHolder().getIdentifier().getColumn(), newLinkingId);

                    collectionEntryHolders.remove(dummyCollection.getKey(), oldIdentifier);
                    collectionEntryHolders.put(dummyCollection.getKey(), newIdentifier);

                    logger.trace("Updated collection entry holder in map: {} -> {} to {} -> {}", dummyCollection.getKey(), oldIdentifier, dummyCollection.getKey(), newIdentifier);
                });
                case DELETE -> dummyCollections.forEach(dummyCollection ->
                        collectionEntryHolders.remove(dummyCollection.getKey(), dummyCollection.getRootHolder().getIdentifier()));
            }
        });
    }

    public static void instantiate(DataManager dataManager, PostgresListener pgListener) {
        instance = new PersistentCollectionManager(dataManager, pgListener);
    }

    public static PersistentCollectionManager getInstance() {
        return instance;
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
                    dataManager.cache(entryDataKey, dataValue, Instant.now());

                    logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, dataValue);
                }

                logger.trace("Adding collection entry holder to map: {} -> {}", dummyCollection.getKey(), uniqueIdentifier);

                dataManager.cache(entryLinkKey, linkingId, Instant.now());
                collectionEntryHolders.put(dummyCollection.getKey(), uniqueIdentifier);
            }
        }
    }

    public void addEntries(PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) {
        UniqueData holder = collection.getRootHolder();
        List<KeyedCollectionEntry> keyedEntries = new ArrayList<>();

        for (CollectionEntry entry : entries) {
            UniqueIdentifier uniqueIdentifier = UniqueIdentifier.of(holder.getIdentifier().getColumn(), entry.id());
            CellKey entryDataKey = getEntryDataKey(collection, entry.id(), holder.getIdentifier());
            CellKey entryLinkKey = getEntryLinkingKey(collection, entry.id(), holder.getIdentifier());

            logger.trace("Adding collection entry to {}", collection.getKey());
            logger.trace("Adding collection entry link to cache: {} -> {}", entryLinkKey, collection.getRootHolder().getId());
            logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, entry.value());
            logger.trace("Adding collection entry holder to map: {} -> {}", collection.getKey(), uniqueIdentifier);

            dataManager.cache(entryLinkKey, collection.getRootHolder().getId(), Instant.now());
            dataManager.cache(entryDataKey, entry.value(), Instant.now());
            collectionEntryHolders.put(collection.getKey(), uniqueIdentifier);

            keyedEntries.add(new KeyedCollectionEntry(entryDataKey, entry.value()));
        }

        ThreadUtils.submit(() -> {
            try {
                addToDatabase(collection, keyedEntries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void removeEntry(PersistentValueCollection<?> collection, KeyedCollectionEntry entry) {
        removeEntries(collection, Collections.singletonList(entry));
    }

    public void removeEntries(PersistentValueCollection<?> collection, List<KeyedCollectionEntry> entries) {
        DataKey collectionKey = collection.getKey();
        for (KeyedCollectionEntry entry : entries) {
            collectionEntryHolders.remove(collectionKey, collection.getRootHolder().getIdentifier());
            dataManager.uncache(entry.dataKey());
        }

        ThreadUtils.submit(() -> removeFromDatabase(collection, entries));
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

    public List<KeyedCollectionEntry> getKeyedEntries(PersistentCollection<?> collection) {
        return collectionEntryHolders.get(collection.getKey()).stream().map(pkey -> {
            CellKey entryDataKey = getEntryDataKey(collection, pkey.getId(), collection.getRootHolder().getIdentifier());
            Object value = dataManager.get(entryDataKey);
            return new KeyedCollectionEntry(entryDataKey, value);
        }).toList();
    }

    public void addToDatabase(PersistentValueCollection<?> collection, List<KeyedCollectionEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        String entryIdColumn = collection.getEntryIdColumn();

        try (Connection connection = dataManager.getConnection()) {
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

                for (KeyedCollectionEntry entry : entries) {
                    UniqueIdentifier pkey = collection.getRootHolder().getIdentifier();

                    int i = 1;
                    statement.setObject(i, pkey.getId());
                    if (!collection.getDataColumn().equals(entryIdColumn)) {
                        // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
                        statement.setObject(++i, dataManager.serialize(entry.value()));
                    }
                    statement.setObject(++i, collection.getRootHolder().getId());

                    statement.executeUpdate();
                }

            } finally {
                connection.commit();
                connection.setAutoCommit(autoCommit);
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void removeFromDatabase(PersistentValueCollection<?> collection, List<KeyedCollectionEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        UniqueIdentifier firstPkey = collection.getRootHolder().getIdentifier();

        try (Connection connection = dataManager.getConnection()) {
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

                for (KeyedCollectionEntry entry : entries) {
                    UniqueIdentifier pkey = collection.getRootHolder().getIdentifier();
                    statement.setObject(i++, pkey.getId());
                }

                statement.executeUpdate();
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
