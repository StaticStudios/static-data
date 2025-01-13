package net.staticstudios.data.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.PersistentValue;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.collection.*;
import net.staticstudios.data.impl.pg.PostgresListener;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.key.UniqueIdentifier;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Blocking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PersistentCollectionManager extends SQLLogger {
    private final Logger logger = LoggerFactory.getLogger(PersistentCollectionManager.class);
    private final DataManager dataManager;
    private final PostgresListener pgListener;
    private final Multimap<CollectionKey, PersistentCollectionChangeHandler<?>> addHandlers;
    private final Multimap<CollectionKey, PersistentCollectionChangeHandler<?>> removeHandlers;
    /**
     * Maps a collection key to a list of data keys for each entry in the collection
     */
    private final Multimap<CollectionKey, CollectionEntryIdentifier> collectionEntryHolders;
    private final Map<String, JunctionTable> junctionTables;

    public PersistentCollectionManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;
        this.collectionEntryHolders = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.addHandlers = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
        this.removeHandlers = Multimaps.synchronizedListMultimap(ArrayListMultimap.create());
        this.junctionTables = new ConcurrentHashMap<>();

        pgListener.addHandler(notification -> {
            // The PersistentValueManager will handle updating the main cache
            // All we have to concern ourselves with is updating the collection entry holders
            // Note about updates: we have to care about when the linking column changes, since that's what we use to identify the holder

            Collection<SimplePersistentCollection<?>> dummyCollections = dataManager.getDummyPersistentCollections(notification.getSchema() + "." + notification.getTable());
            switch (notification.getOperation()) {
                case INSERT -> dummyCollections.forEach(dummyCollection -> {
                    String encodedLinkingId = notification.getData().newDataValueMap().get(dummyCollection.getLinkingColumn());
                    if (encodedLinkingId == null) {
                        return;
                    }

                    UUID linkingId = UUID.fromString(encodedLinkingId);
                    UUID entryId = UUID.fromString(notification.getData().newDataValueMap().get(dummyCollection.getEntryIdColumn()));

                    if (dummyCollection instanceof PersistentValueCollection<?> vc) {
                        String encoded = notification.getData().newDataValueMap().get(dummyCollection.getDataColumn());
                        Object decoded = dataManager.decode(dummyCollection.getDataType(), encoded);
                        Object deserialized = dataManager.deserialize(dummyCollection.getDataType(), decoded);

                        //Ensure that the entry data is there
                        //For other collections, the PersistentValueManager will handle this
                        addEntriesToCache(vc, List.of(new CollectionEntry(entryId, deserialized)), linkingId);
                    } else if (dummyCollection instanceof PersistentUniqueDataCollection<?> udc) {
                        //Ensure that the entry data is there
                        //For other collections, the PersistentValueManager will handle this
                        addEntriesToCache(udc.getHolderIds(), List.of(new CollectionEntry(entryId, entryId)), linkingId);
                    }
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


                        //Ensure that the entry data is gone
                        //For other collections, the PersistentValueManager will handle this
                        if (dummyCollection instanceof PersistentValueCollection<?>) {
                            removeEntriesFromCache(collectionKey, dummyCollection, List.of(entryId));
                        } else if (dummyCollection instanceof PersistentUniqueDataCollection<?> persistentUniqueDataCollection) {
                            PersistentValueCollection<UUID> dummyIdsCollection = persistentUniqueDataCollection.getHolderIds();
                            CollectionKey idsCollectionKey = new CollectionKey(
                                    dummyIdsCollection.getSchema(),
                                    dummyIdsCollection.getTable(),
                                    dummyIdsCollection.getLinkingColumn(),
                                    dummyIdsCollection.getDataColumn(),
                                    oldLinkingId
                            );
                            removeFromUniqueDataCollectionInMemory(idsCollectionKey, dummyIdsCollection, List.of(entryId));
                        }
                    }

                    if (newLinkingId != null) {
                        if (dummyCollection instanceof PersistentValueCollection<?> pvc) {
                            String encoded = notification.getData().newDataValueMap().get(dummyCollection.getDataColumn());
                            Object decoded = dataManager.decode(dummyCollection.getDataType(), encoded);
                            Object deserialized = dataManager.deserialize(dummyCollection.getDataType(), decoded);

                            //Ensure that the entry data is there
                            //For other collections, the PersistentValueManager will handle this
                            addEntriesToCache(pvc, List.of(new CollectionEntry(entryId, deserialized)), newLinkingId);
                        } else if (dummyCollection instanceof PersistentUniqueDataCollection<?> udc) {
                            //Ensure that the entry data is there
                            //For other collections, the PersistentValueManager will handle this
                            addEntriesToCache(udc.getHolderIds(), List.of(new CollectionEntry(entryId, entryId)), newLinkingId);
                        }
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
                    UUID entryId = UUID.fromString(notification.getData().oldDataValueMap().get(dummyCollection.getEntryIdColumn()));

                    //Ensure that the entry data is gone
                    //For other collections, the PersistentValueManager will handle this
                    if (dummyCollection instanceof PersistentValueCollection<?>) {
                        removeEntriesFromCache(collectionKey, dummyCollection, List.of(entryId));
                    } else if (dummyCollection instanceof PersistentUniqueDataCollection<?> persistentUniqueDataCollection) {
                        PersistentValueCollection<UUID> dummyIdsCollection = persistentUniqueDataCollection.getHolderIds();
                        CollectionKey idsCollectionKey = new CollectionKey(
                                dummyIdsCollection.getSchema(),
                                dummyIdsCollection.getTable(),
                                dummyIdsCollection.getLinkingColumn(),
                                dummyIdsCollection.getDataColumn(),
                                UUID.fromString(encodedLinkingId)
                        );
                        removeFromUniqueDataCollectionInMemory(idsCollectionKey, dummyIdsCollection, List.of(entryId));
                    }
                });
            }
        });

        //Handle junction table updates
        pgListener.addHandler(notification -> {
            String junctionTable = notification.getSchema() + "." + notification.getTable();
            JunctionTable jt = junctionTables.get(junctionTable);
            if (jt == null) {
                return;
            }

            switch (notification.getOperation()) {
                case INSERT -> {
                    List<String> columns = notification.getData().newDataValueMap().keySet().stream().toList();
                    UUID leftId = UUID.fromString(notification.getData().newDataValueMap().get(columns.get(0)));
                    UUID rightId = UUID.fromString(notification.getData().newDataValueMap().get(columns.get(1)));

                    CollectionKey leftCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            columns.get(0),
                            columns.get(1),
                            leftId
                    );

                    CollectionKey rightCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            columns.get(1),
                            columns.get(0),
                            rightId
                    );

                    dataManager.getDummyPersistentManyToManyCollection(junctionTable).forEach(dummyCollection -> {
                        jt.add(dummyCollection.getThisIdColumn(), leftId, rightId);
                    });

                    callAddHandlers(leftCollectionKey, rightId);
                    callAddHandlers(rightCollectionKey, leftId);
                }
                case UPDATE -> {
                    List<String> oldColumns = notification.getData().oldDataValueMap().keySet().stream().toList();
                    UUID oldLeftId = UUID.fromString(notification.getData().oldDataValueMap().get(oldColumns.get(0)));
                    UUID oldRightId = UUID.fromString(notification.getData().oldDataValueMap().get(oldColumns.get(1)));

                    List<String> newColumns = notification.getData().newDataValueMap().keySet().stream().toList();
                    UUID newLeftId = UUID.fromString(notification.getData().newDataValueMap().get(newColumns.get(0)));
                    UUID newRightId = UUID.fromString(notification.getData().newDataValueMap().get(newColumns.get(1)));

                    CollectionKey oldLeftCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            oldColumns.get(0),
                            oldColumns.get(1),
                            oldLeftId
                    );

                    CollectionKey oldRightCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            oldColumns.get(1),
                            oldColumns.get(0),
                            oldRightId
                    );

                    CollectionKey newLeftCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            newColumns.get(0),
                            newColumns.get(1),
                            newLeftId
                    );

                    CollectionKey newRightCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            newColumns.get(1),
                            newColumns.get(0),
                            newRightId
                    );

                    callRemoveHandlers(oldLeftCollectionKey, oldRightId);
                    callRemoveHandlers(oldRightCollectionKey, oldLeftId);

                    dataManager.getDummyPersistentManyToManyCollection(junctionTable).forEach(dummyCollection -> {
                        jt.remove(dummyCollection.getThisIdColumn(), oldLeftId, oldRightId);
                        jt.add(dummyCollection.getThisIdColumn(), newLeftId, newRightId);
                    });

                    callAddHandlers(newLeftCollectionKey, newRightId);
                    callAddHandlers(newRightCollectionKey, newLeftId);

                }
                case DELETE -> {
                    List<String> columns = notification.getData().oldDataValueMap().keySet().stream().toList();
                    UUID leftId = UUID.fromString(notification.getData().oldDataValueMap().get(columns.get(0)));
                    UUID rightId = UUID.fromString(notification.getData().oldDataValueMap().get(columns.get(1)));

                    CollectionKey leftCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            columns.get(0),
                            columns.get(1),
                            leftId
                    );

                    CollectionKey rightCollectionKey = new CollectionKey(
                            notification.getSchema(),
                            notification.getTable(),
                            columns.get(1),
                            columns.get(0),
                            rightId
                    );

                    callRemoveHandlers(leftCollectionKey, rightId);
                    callRemoveHandlers(rightCollectionKey, leftId);

                    dataManager.getDummyPersistentManyToManyCollection(junctionTable).forEach(dummyCollection -> {
                        jt.remove(dummyCollection.getThisIdColumn(), leftId, rightId);
                    });
                }
            }
        });
    }

    public void deleteFromCache(DeleteContext context) {
        for (Data<?> data : context.toDelete()) {
            if (data instanceof PersistentValueCollection<?> collection) {
                removeEntriesFromCache(collection, getCollectionEntries(collection));
            } else if (data instanceof PersistentUniqueDataCollection<?> collection) {
                if (collection.getDeletionStrategy() == DeletionStrategy.CASCADE) {
                    removeEntriesFromCache(collection, getCollectionEntries(collection));
                } else if (collection.getDeletionStrategy() == DeletionStrategy.UNLINK) {
                    PersistentValueCollection<UUID> ids = collection.getHolderIds();
                    removeFromUniqueDataCollectionInMemory(ids, new ArrayList<>(ids));
                }
            } else if (data instanceof PersistentManyToManyCollection<?> collection) {
                //Note that the individual entries are already in the deletion context via the data manager
                removeFromJunctionTableInMemory(collection, getJunctionTableEntryIds(collection));
            }
        }

        for (Data<?> data : context.toDelete()) {
            if (data instanceof PersistentValue<?> pv) {
                handlePersistentValueDeletionInMemory(pv);
            }
        }
    }

    public void deleteFromDatabase(Connection connection, DeleteContext context) throws SQLException {
        for (Data<?> data : context.toDelete()) {
            if (data instanceof PersistentValueCollection<?> collection) {
                String sql = "DELETE FROM " + collection.getSchema() + "." + collection.getTable() + " WHERE " + collection.getLinkingColumn() + " = ?";
                logSQL(sql);

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, collection.getRootHolder().getId());
                    statement.executeUpdate();
                }
            } else if (data instanceof PersistentUniqueDataCollection<?> collection) {
                if (collection.getDeletionStrategy() == DeletionStrategy.CASCADE) {
                    String sql = "DELETE FROM " + collection.getSchema() + "." + collection.getTable() + " WHERE " + collection.getLinkingColumn() + " = ?";
                    logSQL(sql);

                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, collection.getRootHolder().getId());
                        statement.executeUpdate();
                    }
                } else if (collection.getDeletionStrategy() == DeletionStrategy.UNLINK) {
                    String sql = "UPDATE " + collection.getSchema() + "." + collection.getTable() + " SET " + collection.getLinkingColumn() + " = NULL WHERE " + collection.getLinkingColumn() + " = ?";
                    logSQL(sql);

                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, collection.getRootHolder().getId());
                        statement.executeUpdate();
                    }
                }
            } else if (data instanceof PersistentManyToManyCollection<?> collection) {
                if (collection.getDeletionStrategy() == DeletionStrategy.CASCADE) {
                    UniqueData dummyInstance = dataManager.getDummyInstance(collection.getDataType());
                    String sql = "DELETE FROM " + dummyInstance.getSchema() + "." + dummyInstance.getTable() +
                            " WHERE EXISTS ( SELECT 1 FROM " + collection.getSchema() + "." + collection.getJunctionTable() +
                            " WHERE " + collection.getSchema() + "." + collection.getJunctionTable() + "." + collection.getThisIdColumn() + " = ? AND " +
                            dummyInstance.getSchema() + "." + dummyInstance.getTable() + "." + dummyInstance.getIdentifier().getColumn() +
                            " = " + collection.getSchema() + "." + collection.getJunctionTable() + "." + collection.getThatIdColumn() +
                            ")";
                    logSQL(sql);

                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, collection.getRootHolder().getId());
                        statement.executeUpdate();
                    }
                } else if (collection.getDeletionStrategy() == DeletionStrategy.UNLINK) {
                    String sql = "DELETE FROM " + collection.getSchema() + "." + collection.getJunctionTable() + " WHERE " + collection.getThisIdColumn() + " = ?";
                    logSQL(sql);

                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, collection.getRootHolder().getId());
                        statement.executeUpdate();
                    }
                }
            } else if (data instanceof PersistentValue<?> pv) {
                handlePersistentValueDeletionInDatabase(connection, context, pv);
            }
        }
    }

    private void addEntry(CollectionKey key, CollectionEntryIdentifier identifier) {
        collectionEntryHolders.put(key, identifier);

        Object newValue = getEntry(key, identifier);
        callAddHandlers(key, newValue);
    }

    private void removeEntry(CollectionKey key, CollectionEntryIdentifier identifier) {
        Object oldValue = getEntry(key, identifier);
        callRemoveHandlers(key, oldValue);

        //remove after handlers are called to avoid DDNEEs
        collectionEntryHolders.remove(key, identifier);
    }

    private void callAddHandlers(CollectionKey key, Object newValue) {
        Collection<PersistentCollectionChangeHandler<?>> addHandlers = this.addHandlers.get(key);
        addHandlers.forEach(handler -> {
            try {
                handler.unsafeOnChange(newValue);
            } catch (Exception e) {
                logger.error("Error while handling add event", e);
            }
        });
    }

    private void callRemoveHandlers(CollectionKey key, Object oldValue) {
        Collection<PersistentCollectionChangeHandler<?>> removeHandlers = this.removeHandlers.get(key);
        removeHandlers.forEach(handler -> {
            try {
                handler.unsafeOnChange(oldValue);
            } catch (Exception e) {
                logger.error("Error while handling remove event", e);
            }
        });
    }

    public void addAddHandler(PersistentCollection<?> collection, PersistentCollectionChangeHandler<?> handler) {
        if (collection.getRootHolder().getId() == null) {
            return; //Dummy collection
        }

        addHandlers.put(collection.getKey(), handler);
    }

    public void addRemoveHandler(PersistentCollection<?> collection, PersistentCollectionChangeHandler<?> handler) {
        if (collection.getRootHolder().getId() == null) {
            return; //Dummy collection
        }

        removeHandlers.put(collection.getKey(), handler);
    }

    private void handlePersistentValueDeletionInMemory(PersistentValue<?> pv) {
        //when a PV is uncached, if it is our linking column, we need to remove the entry from the collection
        if (pv.getDataType() != UUID.class) {
            return;
        }

        try {
            if (pv.get() == null) {
                return;
            }
        } catch (DataDoesNotExistException e) {
            return;
        }

        UUID oldLinkingId = (UUID) pv.get();

        dataManager.getDummyPersistentCollections(pv.getSchema() + "." + pv.getTable()).stream()
                .filter(dummyCollection -> dummyCollection.getLinkingColumn().equals(pv.getColumn()))
                .forEach(dummyCollection -> {

                    CollectionEntryIdentifier oldIdentifier = CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), pv.getHolder().getRootHolder().getId());

                    CollectionKey oldCollectionKey = new CollectionKey(
                            dummyCollection.getSchema(),
                            dummyCollection.getTable(),
                            dummyCollection.getLinkingColumn(),
                            dummyCollection.getDataColumn(),
                            oldLinkingId
                    );

                    try {
                        getEntry(oldCollectionKey, oldIdentifier);
                    } catch (DataDoesNotExistException e) {
                        // Ignore, this means that the entry was already deleted
                        // This can happen if we have a cascade deletion and the entry was already deleted via one of collection deletions
                        return;
                    }

                    removeEntry(oldCollectionKey, oldIdentifier);
                    logger.trace("Removed collection entry holder from map: {} -> {}", oldCollectionKey, oldIdentifier);
                });

        UniqueData holder = pv.getHolder().getRootHolder();
        dataManager.getAllDummyPersistentManyToManyCollections().stream()
                .filter(dummyCollection -> dummyCollection.getDataType().equals(holder.getClass()))
                .forEach(dummyCollection -> {
                    JunctionTable jt = junctionTables.get(dummyCollection.getSchema() + "." + dummyCollection.getJunctionTable());
                    jt.removeIf(dummyCollection.getThisIdColumn(), (left, right) -> {
                        if (right.equals(holder.getId())) {
                            CollectionKey collectionKey = new CollectionKey(
                                    dummyCollection.getSchema(),
                                    dummyCollection.getJunctionTable(),
                                    dummyCollection.getThisIdColumn(),
                                    dummyCollection.getThatIdColumn(),
                                    left
                            );

                            callRemoveHandlers(collectionKey, right);
                            return true;
                        }
                        return false;
                    });
                });
    }

    private void handlePersistentValueDeletionInDatabase(Connection connection, DeleteContext context, PersistentValue<?> pv) {
        //when a PV is uncached, if it is our linking column, we need to remove the entry from the collection
        if (pv.getDataType() != UUID.class) {
            return;
        }

        UUID oldEntryId = (UUID) context.oldValues().get(pv.getKey());
        if (oldEntryId == null) {
            return;
        }

        UniqueData holder = pv.getHolder().getRootHolder();
        dataManager.getAllDummyPersistentManyToManyCollections().stream()
                .filter(dummyCollection -> dummyCollection.getDataType().equals(holder.getClass()))
                .forEach(dummyCollection -> {
                    String sql = "DELETE FROM " + dummyCollection.getSchema() + "." + dummyCollection.getJunctionTable() + " WHERE " + dummyCollection.getThatIdColumn() + " = ?";
                    logSQL(sql);

                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setObject(1, oldEntryId);
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
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

                        CollectionEntryIdentifier oldIdentifier = CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), oldEntryId);
                        removeEntry(oldCollectionKey, oldIdentifier);
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

                        CollectionEntryIdentifier newIdentifier = CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), newEntryId);
                        addEntry(newCollectionKey, newIdentifier);
                        logger.trace("Added collection entry holder to map: {} -> {}", newCollectionKey, newIdentifier);
                    }
                });
    }

    @Blocking
    public void loadAllFromDatabase(Connection connection, UniqueData dummyHolder, SimplePersistentCollection<?> dummyCollection) throws SQLException {
        logger.trace("Loading all collection entries for {}", dummyCollection.getKey());
        String schemaTable = dummyCollection.getSchema() + "." + dummyCollection.getTable();
        pgListener.ensureTableHasTrigger(connection, schemaTable);

        Set<String> columns = new HashSet<>();

        String entryIdColumn = dummyCollection.getEntryIdColumn();
        String entryDataColumn = dummyCollection.getDataColumn();
        String collectionLinkingColumn = dummyCollection.getLinkingColumn();

        columns.add(entryIdColumn);
        columns.add(collectionLinkingColumn);
        columns.add(entryDataColumn);

        StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        for (String column : columns) {
            sqlBuilder.append(column).append(", ");
        }
        sqlBuilder.setLength(sqlBuilder.length() - 2);

        sqlBuilder.append(" FROM ").append(schemaTable);

        String sql = sqlBuilder.toString();
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                UUID entryId = (UUID) resultSet.getObject(entryIdColumn);
                UUID linkingId = (UUID) resultSet.getObject(collectionLinkingColumn);

                CollectionKey collectionKey = new CollectionKey(
                        dummyCollection.getSchema(),
                        dummyCollection.getTable(),
                        dummyCollection.getLinkingColumn(),
                        dummyCollection.getDataColumn(),
                        linkingId
                );

                CollectionEntryIdentifier identifier = CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), entryId);
                CellKey entryDataKey = getEntryDataKey(dummyCollection, entryId);
                CellKey entryLinkKey = getEntryLinkingKey(dummyCollection, entryId);


                // I ordered the logs like this so that the output is in the same order as it is elsewhere in this class

                logger.trace("Adding collection entry to {}", collectionKey);
                logger.trace("Adding collection entry link to cache: {} -> {}", entryLinkKey, linkingId);

                if (entryIdColumn.equals(entryDataColumn)) {
                    // For PersistentValueCollection the entry id will be the data, since that's what we're interested in
                    dataManager.cache(entryDataKey, UUID.class, entryId, Instant.now());
                    logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, entryId);
                } else {
                    // For PersistentUniqueDataCollection the entry id will be the data, since that's what we're interested in
                    Object serializedDataValue = resultSet.getObject(entryDataColumn);
                    Object dataValue = dataManager.deserialize(dummyCollection.getDataType(), serializedDataValue);
                    dataManager.cache(entryDataKey, dummyCollection.getDataType(), dataValue, Instant.now());

                    logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, dataValue);
                }

                logger.trace("Adding collection entry holder to map: {} -> {}", collectionKey, identifier);

                dataManager.cache(entryLinkKey, UUID.class, linkingId, Instant.now());
                addEntry(collectionKey, identifier);
            }
        }
    }

    public void addEntriesToCache(PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) {
        UniqueData holder = collection.getRootHolder();
        addEntriesToCache(collection, entries, holder.getId());
    }

    public void addEntriesToCache(PersistentValueCollection<?> dummyCollection, Collection<CollectionEntry> entries, UUID holderId) {
        CollectionKey collectionKey = new CollectionKey(
                dummyCollection.getSchema(),
                dummyCollection.getTable(),
                dummyCollection.getLinkingColumn(),
                dummyCollection.getDataColumn(),
                holderId
        );

        for (CollectionEntry entry : entries) {
            CollectionEntryIdentifier identifier = CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), entry.id());
            CellKey entryDataKey = getEntryDataKey(dummyCollection, entry.id());
            CellKey entryLinkKey = getEntryLinkingKey(dummyCollection, entry.id());

            logger.trace("Adding collection entry to {}", collectionKey);
            logger.trace("Adding collection entry link to cache: {} -> {}", entryLinkKey, holderId);
            logger.trace("Adding collection entry data to cache: {} -> {}", entryDataKey, entry.value());
            logger.trace("Adding collection entry holder to map: {} -> {}", collectionKey, identifier);

            dataManager.cache(entryLinkKey, UUID.class, holderId, Instant.now());
            dataManager.cache(entryDataKey, dummyCollection.getDataType(), entry.value(), Instant.now());
            addEntry(collectionKey, identifier);
        }
    }

    public void removeEntriesFromCache(SimplePersistentCollection<?> collection, List<CollectionEntry> entries) {
        CollectionKey collectionKey = collection.getKey();
        removeEntriesFromCache(collectionKey, collection, entries.stream().map(CollectionEntry::id).toList());
    }

    public void removeEntriesFromCache(CollectionKey collectionKey, SimplePersistentCollection<?> dummyCollection, List<UUID> entryIds) {
        for (UUID entryId : entryIds) {
            removeEntry(collectionKey, CollectionEntryIdentifier.of(dummyCollection.getEntryIdColumn(), entryId));
            dataManager.uncache(getEntryDataKey(dummyCollection, entryId));
            dataManager.uncache(getEntryLinkingKey(dummyCollection, entryId));
        }
    }

    private CellKey getEntryDataKey(SimplePersistentCollection<?> collection, UUID entryId) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getDataColumn(), entryId, collection.getEntryIdColumn());
    }

    public CellKey getEntryLinkingKey(SimplePersistentCollection<?> collection, UUID entryId) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), entryId, collection.getRootHolder().getIdentifier().getColumn());
    }

    public List<CellKey> getEntryKeys(SimplePersistentCollection<?> collection) {
        return collectionEntryHolders.get(collection.getKey()).stream().map(k -> getEntryDataKey(collection, k.getEntryId())).toList();
    }

    public List<Object> getEntries(SimplePersistentCollection<?> collection) {
        return getEntryKeys(collection).stream().map(dataManager::get).toList();
    }

    public Object getEntry(CollectionKey collectionKey, CollectionEntryIdentifier identifier) {
        CellKey entryDataKey = new CellKey(collectionKey.getSchema(), collectionKey.getTable(), collectionKey.getDataColumn(), identifier.getEntryId(), identifier.getEntryIdColumn());
        return dataManager.get(entryDataKey);
    }

    public List<CollectionEntry> getCollectionEntries(SimplePersistentCollection<?> collection) {
        return collectionEntryHolders.get(collection.getKey()).stream().map(identifier -> {
            CellKey entryDataKey = getEntryDataKey(collection, identifier.getEntryId());
            Object value = dataManager.get(entryDataKey);
            return new CollectionEntry(identifier.getEntryId(), value);
        }).toList();
    }

    public void addValueEntryToDatabase(Connection connection, PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) throws SQLException {
        if (entries.isEmpty()) {
            return;
        }

        String entryIdColumn = collection.getEntryIdColumn();

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" (");

        List<String> columns = new ArrayList<>();

        columns.add(entryIdColumn);
        if (!collection.getDataColumn().equals(entryIdColumn)) {
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
        logSQL(sql);


        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (CollectionEntry entry : entries) {

                int i = 1;
                statement.setObject(i, entry.id());
                if (!collection.getDataColumn().equals(entryIdColumn)) {
                    statement.setObject(++i, dataManager.serialize(entry.value()));
                }
                statement.setObject(++i, collection.getRootHolder().getId());

                statement.executeUpdate();
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }

    }

    public void addUniqueDataEntryToDatabase(Connection connection, PersistentUniqueDataCollection<?> collection, Collection<CollectionEntry> entries) throws SQLException {
        if (entries.isEmpty()) {
            return;
        }

        StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" SET ");
        sqlBuilder.append(collection.getLinkingColumn()).append(" = ? WHERE ");
        sqlBuilder.append(collection.getEntryIdColumn()).append(" = ?");

        String sql = sqlBuilder.toString();
        logSQL(sql);

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (CollectionEntry entry : entries) {
                int i = 1;
                statement.setObject(i, collection.getRootHolder().getId());
                statement.setObject(++i, entry.id());

                statement.executeUpdate();
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }


    public void removeValueEntryFromDatabase(Connection connection, PersistentValueCollection<?> collection, List<CollectionEntry> entries) throws SQLException {
        if (entries.isEmpty()) {
            return;
        }

        UniqueIdentifier holderIdentifier = collection.getRootHolder().getIdentifier();
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" WHERE (");

        sqlBuilder.append(holderIdentifier.getColumn());

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
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;

            for (CollectionEntry entry : entries) {
                statement.setObject(i++, entry.id());
            }

            statement.executeUpdate();
        }
    }

    public void removeFromUniqueDataCollectionInMemory(PersistentValueCollection<?> idsCollection, List<UUID> entryIds) {
        CollectionKey idsCollectionKey = idsCollection.getKey();
        removeFromUniqueDataCollectionInMemory(idsCollectionKey, idsCollection, entryIds);
    }

    public void removeFromUniqueDataCollectionInMemory(CollectionKey idsCollectionKey, PersistentValueCollection<?> dummyIdsCollection, List<UUID> entryIds) {
        if (entryIds.isEmpty()) {
            return;
        }

        for (UUID entry : entryIds) {
            CollectionEntryIdentifier identifier = CollectionEntryIdentifier.of(dummyIdsCollection.getEntryIdColumn(), entry);
            callRemoveHandlers(idsCollectionKey, getEntry(idsCollectionKey, identifier));

            CellKey linkingKey = getEntryLinkingKey(dummyIdsCollection, entry);
            dataManager.cache(linkingKey, UUID.class, null, Instant.now());

            //remove after handlers are called to avoid DDNEEs
            collectionEntryHolders.remove(idsCollectionKey, identifier);
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
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;

            for (UUID entry : entryIds) {
                statement.setObject(i++, entry);
            }

            statement.executeUpdate();
        }
    }


    @Blocking
    public void loadJunctionTablesFromDatabase(Connection connection, PersistentManyToManyCollection<?> dummyMMCollection) throws SQLException {
        String junctionTable = dummyMMCollection.getSchema() + "." + dummyMMCollection.getJunctionTable();
        JunctionTable jt = this.junctionTables.computeIfAbsent(junctionTable, k -> new JunctionTable(dummyMMCollection.getThisIdColumn(), dummyMMCollection.getThatIdColumn()));
        pgListener.ensureTableHasTrigger(connection, junctionTable);
        String sql = "SELECT * FROM " + junctionTable;
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            Preconditions.checkArgument(resultSet.getMetaData().getColumnCount() == 2);
            String leftColumn = dummyMMCollection.getThisIdColumn();
            String rightColumn = dummyMMCollection.getThatIdColumn();

            while (resultSet.next()) {
                UUID leftId = (UUID) resultSet.getObject(leftColumn);
                UUID rightId = (UUID) resultSet.getObject(rightColumn);

                jt.add(leftColumn, leftId, rightId);
            }
        }
    }

    public void addToJunctionTableInMemory(PersistentManyToManyCollection<?> collection, List<UUID> ids) {
        String junctionTable = collection.getSchema() + "." + collection.getJunctionTable();
        JunctionTable jt = junctionTables.get(junctionTable);
        Preconditions.checkNotNull(jt, "Junction table not loaded: " + junctionTable);

        for (UUID id : ids) {
            jt.add(collection.getThisIdColumn(), collection.getRootHolder().getId(), id);
        }

        for (UUID id : ids) {
            callAddHandlers(collection.getKey(), id);

            CollectionKey otherCollectionKey = new CollectionKey(
                    collection.getSchema(),
                    collection.getJunctionTable(),
                    collection.getThatIdColumn(),
                    collection.getThisIdColumn(),
                    id
            );
            callAddHandlers(otherCollectionKey, collection.getRootHolder().getId());
        }
    }

    public void removeFromJunctionTableInMemory(PersistentManyToManyCollection<?> collection, List<UUID> ids) {
        String junctionTable = collection.getSchema() + "." + collection.getJunctionTable();
        JunctionTable jt = junctionTables.get(junctionTable);
        Preconditions.checkNotNull(jt, "Junction table not loaded: " + junctionTable);

        for (UUID id : ids) {
            callRemoveHandlers(collection.getKey(), id);

            CollectionKey otherCollectionKey = new CollectionKey(
                    collection.getSchema(),
                    collection.getJunctionTable(),
                    collection.getThatIdColumn(),
                    collection.getThisIdColumn(),
                    id
            );
            callRemoveHandlers(otherCollectionKey, collection.getRootHolder().getId());
        }

        //remove after handlers are called to avoid DDNEEs
        for (UUID id : ids) {
            jt.remove(collection.getThisIdColumn(), collection.getRootHolder().getId(), id);
        }
    }

    public List<UUID> getJunctionTableEntryIds(PersistentManyToManyCollection<?> collection) {
        String junctionTable = collection.getSchema() + "." + collection.getJunctionTable();
        JunctionTable jt = junctionTables.get(junctionTable);
        Preconditions.checkNotNull(jt, "Junction table not loaded: " + junctionTable);

        return jt.get(collection.getThisIdColumn(), collection.getRootHolder().getId()).stream().toList();
    }

    @Blocking
    public void removeFromJunctionTableInDatabase(Connection connection, PersistentManyToManyCollection<?> collection, List<UUID> ids) throws SQLException {
        String junctionTable = collection.getSchema() + "." + collection.getJunctionTable();
        if (ids.isEmpty()) {
            return;
        }

        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(junctionTable).append(" WHERE ");
        sqlBuilder.append(collection.getThisIdColumn()).append(" = ? AND ");
        sqlBuilder.append(collection.getThatIdColumn()).append(" IN (");

        int totalValues = ids.size();

        for (int i = 0; i < totalValues; i++) {
            sqlBuilder.append("?");
            if (i < totalValues - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, collection.getRootHolder().getId());

            for (UUID id : ids) {
                statement.setObject(i++, id);
            }

            statement.executeUpdate();
        }
    }

    @Blocking
    public void addToJunctionTableInDatabase(Connection connection, PersistentManyToManyCollection<?> collection, List<UUID> ids) throws SQLException {
        String junctionTable = collection.getSchema() + "." + collection.getJunctionTable();
        if (ids.isEmpty()) {
            return;
        }

        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(junctionTable).append(" (");
        sqlBuilder.append(collection.getThisIdColumn()).append(", ").append(collection.getThatIdColumn()).append(") VALUES ");

        int totalValues = ids.size();

        for (int i = 0; i < totalValues; i++) {
            sqlBuilder.append("(?, ?)");
            if (i < totalValues - 1) {
                sqlBuilder.append(", ");
            }
        }

        String sql = sqlBuilder.toString();
        logSQL(sql);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;

            for (UUID id : ids) {
                statement.setObject(i++, collection.getRootHolder().getId());
                statement.setObject(i++, id);
            }

            statement.executeUpdate();
        }
    }

}
