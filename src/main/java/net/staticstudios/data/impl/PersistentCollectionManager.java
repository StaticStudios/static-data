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
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.CollectionKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.data.key.UniqueIdentifier;
import net.staticstudios.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class PersistentCollectionManager {
    private static PersistentCollectionManager instance;
    private final Logger logger = LoggerFactory.getLogger(PersistentCollectionManager.class);
    private final DataManager dataManager;
    private final PostgresListener pgListener; //todo: update tables on this when a collection is added
    /**
     * Maps a collection key to a list of data keys for each entry in the collection
     */
    private final Multimap<CollectionKey, UniqueIdentifier> entryMap; //todo: rename

    protected PersistentCollectionManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;
        this.entryMap = Multimaps.synchronizedMultimap(ArrayListMultimap.create());


        pgListener.addHandler(notification -> {
            //todo: something
        });
    }

    public static void instantiate(DataManager dataManager, PostgresListener pgListener) {
        instance = new PersistentCollectionManager(dataManager, pgListener);
    }

    public static PersistentCollectionManager getInstance() {
        return instance;
    }

    public void addEntries(PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) {
        UniqueData holder = collection.getRootHolder();
        List<KeyedCollectionEntry> keyedEntries = new ArrayList<>();

        for (CollectionEntry entry : entries) {
            UniqueIdentifier uniqueIdentifier = UniqueIdentifier.of(holder.getIdentifier().getColumn(), entry.id());
            CellKey entryDataKey = getEntryDataKey(collection, entry.id(), holder.getIdentifier());
            CellKey entryLinkKey = getEntryLinkingKey(collection, entry.id(), holder.getIdentifier());

            logger.trace("Adding collection entry to {}", collection.getKey());
            logger.trace("Linking entry: {} -> {}", entryLinkKey, collection.getRootHolder().getId());
            logger.trace("Adding entry: {} -> {}", entryDataKey, entry.value());

            dataManager.cache(entryLinkKey, collection.getRootHolder().getId(), Instant.now());
            entryMap.put(collection.getKey(), uniqueIdentifier);
            dataManager.cache(entryDataKey, entry.value(), Instant.now());

            keyedEntries.add(new KeyedCollectionEntry(entryDataKey, entry.value()));
        }

        ThreadUtils.submit(() -> {
            try {
                addToDataSource(collection, keyedEntries);
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
            entryMap.remove(collectionKey, collection.getRootHolder().getIdentifier());
            dataManager.uncache(entry.dataKey());
        }

        ThreadUtils.submit(() -> removeFromDataSource(collection, entries));
    }


    public CellKey getEntryDataKey(PersistentCollection<?> collection, UUID entryId, UniqueIdentifier holderIdentifier) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getDataColumn(), entryId, holderIdentifier.getColumn());
    }

    public CellKey getEntryLinkingKey(PersistentCollection<?> collection, UUID entryId, UniqueIdentifier holderIdentifier) {
        return new CellKey(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), entryId, holderIdentifier.getColumn());
    }

    public List<CellKey> getEntryKeys(PersistentCollection<?> collection) {
        return entryMap.get(collection.getKey()).stream().map(k -> getEntryDataKey(collection, k.getId(), collection.getRootHolder().getIdentifier())).toList();
    }

    public List<Object> getEntries(PersistentCollection<?> collection) {
        return getEntryKeys(collection).stream().map(dataManager::get).toList();
    }

    public List<KeyedCollectionEntry> getKeyedEntries(PersistentCollection<?> collection) {
        return entryMap.get(collection.getKey()).stream().map(pkey -> {
            CellKey entryDataKey = getEntryDataKey(collection, pkey.getId(), collection.getRootHolder().getIdentifier());
            Object value = dataManager.get(entryDataKey);
            return new KeyedCollectionEntry(entryDataKey, value);
        }).toList();
    }

    public void addToDataSource(PersistentValueCollection<?> collection, List<KeyedCollectionEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        UniqueIdentifier collectionPkey = collection.getRootHolder().getIdentifier();

        try (Connection connection = dataManager.getConnection()) {
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(collection.getSchema()).append(".").append(collection.getTable()).append(" (");

            List<String> columns = new ArrayList<>();

            columns.add(collectionPkey.getColumn());
            //For PersistentUniqueDataCollection they will be the same
            if (!collection.getDataColumn().equals(collectionPkey.getColumn())) {
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
            sqlBuilder.append(collectionPkey.getColumn());
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
                    if (!collection.getDataColumn().equals(collectionPkey.getColumn())) { //todo: idk about this line
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


    public void removeFromDataSource(PersistentValueCollection<?> collection, List<KeyedCollectionEntry> entries) {
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
