package net.staticstudios.data.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PrimaryKey;
import net.staticstudios.data.data.collection.*;
import net.staticstudios.data.key.CellKey;
import net.staticstudios.data.key.DataKey;
import net.staticstudios.utils.ThreadUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class PersistentCollectionManager implements DataTypeManager<PersistentValueCollection<?>, InitialPersistentCollectionValueEntryData> {
    private final DataManager dataManager;
    private final PostgresListener pgListener;

    /**
     * Maps a collection key to a list of data keys for each entry in the collection
     */
    private final Multimap<DataKey, PrimaryKey> entryMap;

    public PersistentCollectionManager(DataManager dataManager, PostgresListener pgListener) {
        this.dataManager = dataManager;
        this.pgListener = pgListener;
        this.entryMap = Multimaps.synchronizedMultimap(ArrayListMultimap.create());


        pgListener.addHandler(notification -> {
            //todo: something
        });
    }

    public CellKey getEntryDataKey(PersistentCollection<?> collection, UUID entryId) {
        CellKey k = new CellKey(collection.getSchema(), collection.getTable(), collection.getDataColumn(), entryId, "id");
        System.out.println("Entry data key: " + k);
        return k;
    }

    public void addEntries(PersistentValueCollection<?> collection, Collection<CollectionEntry> entries) {
        List<KeyedEntry> keyedEntries = new ArrayList<>();

        for (CollectionEntry entry : entries) {
            PrimaryKey primaryKey = PrimaryKey.of("id", entry.id());
            System.out.println(collection.getLinkingColumn());
            System.out.println(collection.getDataColumn());
            CellKey entryDataKey = getEntryDataKey(collection, entry.id());
            CellKey entryLinkKey = new CellKey(collection.getSchema(), collection.getTable(), collection.getLinkingColumn(), entry.id(), "id");

            System.out.println("linking entry: " + entryLinkKey + " -> " + collection.getRootHolder().getId());

            dataManager.cache(entryLinkKey, collection.getRootHolder().getId());

            keyedEntries.add(new KeyedEntry(primaryKey, entryDataKey, entry.value()));

            System.out.println("Adding entry: " + entryDataKey + " -> " + entry.value());

            System.out.println(collection.getKey());
            System.out.println(primaryKey);

            entryMap.put(collection.getKey(), primaryKey);
            dataManager.cache(entryDataKey, entry.value());
        }

        ThreadUtils.submit(() -> {
            try {
                addToDataSource(collection, keyedEntries);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void removeEntry(PersistentValueCollection<?> collection, KeyedEntry entry) {
        removeEntries(collection, Collections.singletonList(entry));
    }

    public void removeEntries(PersistentValueCollection<?> collection, List<KeyedEntry> entries) {
        DataKey collectionKey = collection.getKey();
        for (KeyedEntry entry : entries) {
            entryMap.remove(collectionKey, entry.pkey());
            dataManager.uncache(entry.dataKey());
        }

        ThreadUtils.submit(() -> removeFromDataSource(collection, entries));
    }

    public List<CellKey> getEntryKeys(PersistentCollection<?> collection) {
        return entryMap.get(collection.getKey()).stream().map(k -> getEntryDataKey(collection, k.getId())).toList();
    }

    public List<Object> getEntries(PersistentCollection<?> collection) {
        return getEntryKeys(collection).stream().map(dataManager::get).toList();
    }

    public List<KeyedEntry> getKeyedEntries(PersistentCollection<?> collection) {
        return entryMap.get(collection.getKey()).stream().map(pkey -> {
            CellKey entryDataKey = getEntryDataKey(collection, pkey.getId());
            Object value = dataManager.get(entryDataKey);
            return new KeyedEntry(pkey, entryDataKey, value);
        }).toList();
    }

    public void addToDataSource(PersistentValueCollection<?> collection, List<KeyedEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        PrimaryKey collectionPkey = collection.getRootHolder().getPKey();

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
            System.out.println(sql);


            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                for (KeyedEntry entry : entries) {
                    PrimaryKey pkey = entry.pkey();

                    int i = 1;
                    statement.setObject(i, pkey.getId());
                    if (!collection.getDataColumn().equals(collectionPkey.getColumn())) { //todo: idk about this line
                        statement.setObject(++i, entry.value());
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


    public void removeFromDataSource(PersistentValueCollection<?> collection, List<KeyedEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        PrimaryKey firstPkey = entries.getFirst().pkey();

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
            System.out.println(sql);
            //todo: log

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int i = 1;

                for (KeyedEntry entry : entries) {
                    PrimaryKey pkey = entry.pkey();
                    statement.setObject(i++, pkey.getId());
                }

                statement.executeUpdate();
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
