package net.staticstudios.data;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.staticstudios.data.messaging.DataLookupMessage;
import net.staticstudios.data.messaging.ForeignLinkUpdateMessage;
import net.staticstudios.data.messaging.handle.*;
import net.staticstudios.data.meta.CachedValueMetadata;
import net.staticstudios.data.meta.Metadata;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.meta.persistant.collection.PersistentCollectionMetadata;
import net.staticstudios.data.meta.persistant.collection.PersistentEntryValueMetadata;
import net.staticstudios.data.meta.persistant.value.AbstractPersistentValueMetadata;
import net.staticstudios.data.meta.persistant.value.ForeignPersistentValueMetadata;
import net.staticstudios.data.meta.persistant.value.PersistentValueMetadata;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.shared.DataWrapper;
import net.staticstudios.data.value.*;
import net.staticstudios.messaging.Messenger;
import net.staticstudios.utils.*;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * The DataManager is responsible for managing all data operations.
 * This includes inserting, updating, and deleting data from various data sources.
 * The DataManager will keep data in sync across all servers.
 */
public class DataManager implements JedisProvider, UniqueServerIdProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataManager.class);
    private final String uniqueServerId;
    private final HikariDataSource dataSource;
    private final String jedisHost;
    private final int jedisPort;
    private final List<DataProvider<?>> dataProviders = new ArrayList<>();
    private final List<UniqueDataMetadata> uniqueDataMetadata = new ArrayList<>();
    private final JedisPool jedisPool;
    private final List<ValueSerializer<?, ?>> SERIALIZERS = new ArrayList<>();
    private final Multimap<String, DataWrapper> dataWrapperLookupTable;
    private final Multimap<String, Metadata> metadataLookupTable;
    private final Multimap<String, ForeignPersistentValueMetadata> foreignLinkLookupTable;
    private Messenger messenger;

    /**
     * Create a new DataManager.
     * <br>
     * Before use, ensure to call {@link DataManager#setMessenger(Messenger)}.
     *
     * @param uniqueServerId The server id, unique to this instance
     * @param hikariConfig   The HikariConfig to use for the connection pool
     * @param redisHost      The host of the Redis server
     * @param redisPort      The port of the Redis server
     */
    public DataManager(
            String uniqueServerId,
            HikariConfig hikariConfig,
            String redisHost,
            int redisPort
    ) {
        this.uniqueServerId = uniqueServerId;
        this.jedisHost = redisHost;
        this.jedisPort = redisPort;

        logger.debug("DataManager created. Unique server ID: {}", uniqueServerId);

        logger.debug("Creating new connection pool");

        dataSource = new HikariDataSource(hikariConfig);
        jedisPool = new JedisPool(redisHost, redisPort);

        dataWrapperLookupTable = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
        metadataLookupTable = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
        foreignLinkLookupTable = Multimaps.synchronizedMultimap(ArrayListMultimap.create());

        ThreadUtils.onShutdownRunAsync(ShutdownStage.CLEANUP, () -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ThreadUtils.submit(() -> {
                future.complete(null);
            });

            return future;
        });
        ThreadUtils.onShutdownRunAsync(ShutdownStage.FINAL, () -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ThreadUtils.submit(() -> {
                dataSource.close();
                future.complete(null);
            });

            return future;
        });
    }

    //todo: jd
    public static void debug(String log) {
        logger.debug(log);
    }

    public static Logger getLogger() {
        return logger;
    }


    //Begin lookup table shenanigans
    public void addDataWrapperToLookupTable(DataWrapper wrapper) {
        addDataWrapperToLookupTable(wrapper.getDataAddress(), wrapper);
    }

    public void removeDataWrapperFromLookupTable(DataWrapper wrapper) {
        removeDataWrapperFromLookupTable(wrapper.getDataAddress(), wrapper);
    }

    public void addDataWrapperToLookupTable(String address, DataWrapper wrapper) {
        if (address == null) {
            return;
        }

        dataWrapperLookupTable.put(address, wrapper);
    }

    public void removeDataWrapperFromLookupTable(String address, DataWrapper wrapper) {
        if (address == null) {
            return;
        }

        dataWrapperLookupTable.remove(address, wrapper);
    }

    public Collection<DataWrapper> getDataWrappers(String address) {
        return dataWrapperLookupTable.get(address);
    }

    public Collection<DataWrapper> extractDataWrappers(UniqueData data) {
        UniqueDataMetadata metadata = getUniqueDataMetadata(data.getClass());

        List<DataWrapper> wrappers = new ArrayList<>();

        Set<Metadata> memberMetadata = metadata.getMemberMetadata();

        for (Metadata member : memberMetadata) {
            wrappers.add(member.getWrapper(data));
        }

        return wrappers;
    }

    public void insertIntoDataWrapperLookupTable(UniqueData data) {
        Collection<DataWrapper> wrappers = extractDataWrappers(data);

        for (DataWrapper wrapper : wrappers) {
            addDataWrapperToLookupTable(wrapper);
        }
    }

    public void removeFromDataWrapperLookupTable(UniqueData data) {
        Collection<DataWrapper> wrappers = extractDataWrappers(data);

        for (DataWrapper wrapper : wrappers) {
            removeDataWrapperFromLookupTable(wrapper);
        }
    }

    public void dumpLookupTable() {
        logger.debug("----------[{}] LOOKUP TABLE DUMP----------", uniqueServerId);

        for (Map.Entry<String, Collection<DataWrapper>> entry : dataWrapperLookupTable.asMap().entrySet()) {
            logger.debug("Lookup table entry: {}", entry.getKey());
            for (DataWrapper wrapper : entry.getValue()) {
                logger.debug("  - {}", wrapper);
            }
        }
    }

    public void addMetadataToLookupTable(Metadata metadata) {
        String address = metadata.getMetadataAddress();

        metadataLookupTable.put(address, metadata);
    }

    public void removeMetadataFromLookupTable(Metadata metadata) {
        String address = metadata.getMetadataAddress();

        metadataLookupTable.remove(address, metadata);
    }

    public Collection<Metadata> getMetadata(String address) {
        return metadataLookupTable.get(address);
    }

    public void addForeignLinkToLookupTable(ForeignPersistentValueMetadata fpvMeta) {
        String linkingTable = fpvMeta.getLinkingTable();

        foreignLinkLookupTable.put(linkingTable, fpvMeta);
    }

    public void removeForeignLinkFromLookupTable(ForeignPersistentValueMetadata fpvMeta) {
        String linkingTable = fpvMeta.getLinkingTable();

        foreignLinkLookupTable.remove(linkingTable, fpvMeta);
    }

    public Collection<ForeignPersistentValueMetadata> getForeignLinks(String linkingTable) {
        return foreignLinkLookupTable.get(linkingTable);
    }
    //End lookup table shenanigans

    /**
     * Set the messenger instance to use for this DataManager.
     *
     * @param messenger The messenger to use
     * @throws IllegalStateException If the messenger is already set
     */
    public void setMessenger(Messenger messenger) {
        if (this.messenger != null) {
            throw new IllegalStateException("Messenger is already set!");
        }

        messenger.registerHandler(new DataInsertMessageHandler(this));
        messenger.registerHandler(new DataValueUpdateMessageHandler(this));
        messenger.registerHandler(new DataDeleteAllMessageHandler(this));
        messenger.registerHandler(new PersistentCollectionAddMessageHandler(this));
        messenger.registerHandler(new PersistentCollectionRemoveMessageHandler(this));
        messenger.registerHandler(new PersistentCollectionEntryUpdateMessageHandler(this));
        messenger.registerHandler(new ForeignLinkUpdateMessageHandler(this));

        messenger.addPatternFilter(this::shouldHandleMessage);

        this.messenger = messenger;
    }

    /**
     * Get the messenger instance.
     *
     * @return The messenger
     * @throws IllegalStateException If the messenger is not set
     */
    public Messenger getMessenger() {
        if (messenger == null) {
            throw new IllegalStateException("Messenger is not set! Make sure to call DataManager#setMessenger!");
        }

        return messenger;
    }

    /**
     * Get a database connection from the pool.
     *
     * @return The connection
     */
    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    @Override
    public String getJedisHost() {
        return jedisHost;
    }

    @Override
    public int getJedisPort() {
        return jedisPort;
    }

    @Override
    public String getServerId() {
        return uniqueServerId;
    }

    /**
     * Register a data provider.
     * Note that the type of provider must be the highest level class of the data type.
     * Super classes will be handled, but the provider must be registered with the highest level class.
     *
     * @param provider The provider to register
     */
    public <T extends UniqueData> void registerDataProvider(DataProvider<T> provider) {
        for (DataProvider<?> registeredProvider : dataProviders) {
            if (registeredProvider.getClass().equals(provider.getClass())) {
                throw new IllegalArgumentException("A provider for " + provider.getDataType().getName() + " is already registered!");
            }
            if (registeredProvider.getDataType().isAssignableFrom(provider.getDataType())) {
                throw new IllegalArgumentException("A provider for " + provider.getDataType().getName() + " (super) is already registered!");
            }
        }

        dataProviders.add(provider);

        UniqueDataMetadata metadata = UniqueDataMetadata.extract(this, provider.getDataType());
        uniqueDataMetadata.add(metadata);

        for (Metadata memberMetadata : metadata.getMemberMetadata()) {
            addMetadataToLookupTable(memberMetadata);

            if (memberMetadata instanceof ForeignPersistentValueMetadata fpvMeta) {
                addForeignLinkToLookupTable(fpvMeta);
            }
        }
    }

    public String getDataChannel(DataWrapper dataWrapper) {
        String address = dataWrapper.getDataAddress();

        return getDataChannel(address);
    }

    public String getDataChannel(String dataAddress) {

        if (dataAddress == null) {
            throw new IllegalArgumentException("Data address is null!");
        }

        return "messages-data-data_address-" + dataAddress;
    }

    public String getForeignLinkChannel(ForeignPersistentValueMetadata fpvMeta) {
        return "messages-data-foreign_link-" + fpvMeta.getLinkingTable();
    }

    public String getMetadataChannel(Metadata metadata) {
        return "messages-data-metadata_address-" + metadata.getMetadataAddress();
    }


    /**
     * Get the channel for the given data.
     * The channel will contain all tables that the data is stored in.
     *
     * @param data The data to get the channel for
     * @return The channel
     */
    public String getChannel(UniqueData data) {
        UniqueDataMetadata metadata = getUniqueDataMetadata(data.getClass());

        StringBuilder sb = new StringBuilder("messages-data-data_tables-");

        for (String table : metadata.getTables()) {
            sb.append(table).append(",");
        }

        return sb.toString();
    }

    /**
     * Check if the given channel should be handled by this DataManager.
     *
     * @param channel The channel to check
     * @return True if the channel should be handled
     */
    public boolean shouldHandleMessage(String channel) {
        if (!channel.startsWith("messages-data-")) {
            return false;
        }

        String[] parts = channel.substring("messages-data-".length()).split("-", 2);

        return switch (parts[0]) {
            case "data_address" -> {
                String address = parts[1];
                Collection<DataWrapper> wrappers = getDataWrappers(address);

                yield !wrappers.isEmpty();
            }

            case "metadata_address" -> {
                String address = parts[1];
                Collection<Metadata> metadata = getMetadata(address);

                yield !metadata.isEmpty();
            }

            case "foreign_link" -> {
                String linkingTable = parts[1];
                Collection<ForeignPersistentValueMetadata> foreignLinks = getForeignLinks(linkingTable);

                yield !foreignLinks.isEmpty();
            }

            case "data_tables" -> {
                String[] tables = parts[1].split(",");

                yield getUniqueDataMetadata(List.of(tables)) != null;
            }
            default -> throw new IllegalStateException("Unexpected value: " + parts[0]);
        };
    }

    /**
     * Get a data provider for the given data type.
     *
     * @param clazz The class of the data type
     * @return The provider
     * @throws IllegalArgumentException If no provider is found
     */
    @SuppressWarnings("unchecked")
    public @NotNull <T extends UniqueData> DataProvider<T> getDataProvider(Class<T> clazz) {
        //Grab the metadata and use the highest level class to find the provider
        UniqueDataMetadata metadata = getUniqueDataMetadata(clazz);
        for (DataProvider<?> provider : dataProviders) {
            if (provider.getDataType().equals(metadata.getType())) {
                return (DataProvider<T>) provider;
            }
        }

        throw new IllegalArgumentException("No provider found for " + clazz.getName());
    }

    public @NotNull UniqueDataMetadata getUniqueDataMetadata(Class<? extends UniqueData> clazz) {
        for (UniqueDataMetadata metadata : uniqueDataMetadata) {
            if (clazz.isAssignableFrom(metadata.getType())) {
                return metadata;
            }
        }

        throw new IllegalArgumentException("No metadata found for " + clazz.getName());
    }

    /**
     * Get the {@link UniqueDataMetadata} for a given table.
     *
     * @param table The table
     * @return The metadata, or null if not found
     */
    public @Nullable UniqueDataMetadata getUniqueDataMetadata(String table) {
        for (UniqueDataMetadata metadata : uniqueDataMetadata) {
            if (metadata.getTables().contains(table)) {
                return metadata;
            }
        }

        return null;
    }

    /**
     * Try to find the {@link UniqueDataMetadata} for a collection of tables.
     * This will return the metadata for the first table found.
     *
     * @param tables The tables to search
     * @return The metadata, or null if not found
     */
    public @Nullable UniqueDataMetadata getUniqueDataMetadata(Collection<String> tables) {
        for (String table : tables) {
            UniqueDataMetadata metadata = getUniqueDataMetadata(table);
            if (metadata != null) {
                return metadata;
            }
        }

        return null;
    }

    /**
     * Select all data of a given type from the data source.
     *
     * @param clazz The class of the data type
     * @return A collection of all data
     * @throws SQLException If an error occurs
     */
    public <T extends UniqueData> Collection<T> selectAll(Class<T> clazz) throws SQLException {
        try (Connection connection = getConnection()) {
            try (Jedis jedis = getJedis()) {
                return selectAll(connection, jedis, clazz);
            }
        }
    }

    /**
     * Select all data of a given type from the data source.
     *
     * @param connection The connection to use
     * @param jedis      The jedis instance to use
     * @param clazz      The class of the data type
     * @return A collection of all data
     * @throws SQLException If an error occurs
     */
    @SuppressWarnings("unchecked")
    public <T extends UniqueData> Collection<T> selectAll(Connection connection, Jedis jedis, Class<T> clazz) throws SQLException {
        UniqueDataMetadata uniqueDataMetadata = getUniqueDataMetadata(clazz);

        return (Collection<T>) select(connection, jedis, uniqueDataMetadata, false, null);
    }

    /**
     * Select some data from the data source.
     *
     * @param uniqueDataMetadata The metadata for the data type
     * @param byId               Whether to select by ID
     * @param id                 The ID to select by
     * @return A collection of data
     * @throws SQLException If an error occurs
     */
    public Collection<UniqueData> select(@NotNull UniqueDataMetadata uniqueDataMetadata, boolean byId, @Nullable UUID id) throws SQLException {
        try (Connection connection = getConnection()) {
            try (Jedis jedis = getJedis()) {
                return select(connection, jedis, uniqueDataMetadata, byId, id);
            }
        }
    }

    private List<AbstractPersistentValueMetadata<?>> getAbstractPersistentValueMetadata(UniqueDataMetadata metadata) {
        List<PersistentValueMetadata> persistentValueMetadataList = metadata.getValueMetadata(PersistentValueMetadata.class);
        List<ForeignPersistentValueMetadata> foreignPersistentValueMetadataList = metadata.getValueMetadata(ForeignPersistentValueMetadata.class);

        List<AbstractPersistentValueMetadata<?>> abstractPersistentValueMetadataList = new ArrayList<>();

        abstractPersistentValueMetadataList.addAll(persistentValueMetadataList);
        abstractPersistentValueMetadataList.addAll(foreignPersistentValueMetadataList);

        return abstractPersistentValueMetadataList;
    }

    /**
     * Select some data from the data source.
     *
     * @param connection         The connection to use
     * @param jedis              The jedis instance to use
     * @param uniqueDataMetadata The metadata for the data type
     * @param byId               Whether to select by ID
     * @param id                 The ID to select by
     * @return A collection of data
     * @throws SQLException If an error occurs
     */
    public Collection<UniqueData> select(Connection connection, Jedis jedis, @NotNull UniqueDataMetadata uniqueDataMetadata, boolean byId, @Nullable UUID id) throws SQLException {
        Map<UUID, UniqueData> results = new HashMap<>();

        String selectValuesStatement = buildSelectValuesStatement(uniqueDataMetadata, byId);

        debug("Executing query: " + selectValuesStatement);
        PreparedStatement preparedStatement = connection.prepareStatement(selectValuesStatement);
        if (byId) {
            preparedStatement.setObject(1, id);
        }

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            UUID instanceId = resultSet.getObject("id", UUID.class);

            //if the top level id is null, we assume we don't have a relevant entry
            if (instanceId == null) {
                continue;
            }

            UniqueData instance = results.get(instanceId);

            if (instance == null) {
                instance = uniqueDataMetadata.createInstance(this);
                uniqueDataMetadata.setIdentifyingValue(instance, instanceId);
                results.put(instanceId, instance);
            }

            List<AbstractPersistentValueMetadata<?>> abstractPersistentValueMetadataList = getAbstractPersistentValueMetadata(uniqueDataMetadata);

            for (AbstractPersistentValueMetadata<?> valueMetadata : abstractPersistentValueMetadataList) {
                Object value = resultSet.getObject(valueMetadata.getColumn());

                if (valueMetadata instanceof ForeignPersistentValueMetadata foreignPersistentValueMetadata) {
                    String foreignObjectIdColumn = foreignPersistentValueMetadata.getLinkingTable() + "." + foreignPersistentValueMetadata.getForeignLinkingColumn();
                    foreignObjectIdColumn = foreignObjectIdColumn.replace(".", "_");
                    UUID foreignId = resultSet.getObject(foreignObjectIdColumn, UUID.class);

                    if (foreignId == null) {
                        continue;
                    }

                    if (value == null) {
                        throw new IllegalStateException("Foreign value is null but foreign id is not null!");
                    }


                    ForeignPersistentValue<?> fpv = foreignPersistentValueMetadata.getSharedValue(instance);
                    fpv.setInternal(value);
                    fpv.setInternalForeignObjectId(foreignId);
                    continue;
                }

                assert value != null;

                Object deserializedValue = deserialize(valueMetadata.getType(), value);
                valueMetadata.setInternalValue(instance, deserializedValue);
            }
        }


        //Collections start
        List<Pair<PersistentCollectionMetadata, String>> selectCollectionsStatementPairs = buildSelectCollectionsStatements(uniqueDataMetadata, byId);
        for (Pair<PersistentCollectionMetadata, String> pair : selectCollectionsStatementPairs) {
            PersistentCollectionMetadata collectionMetadata = pair.first();
            String statement = pair.second();
            debug("Executing query: " + statement);
            preparedStatement = connection.prepareStatement(statement);
            if (byId) {
                preparedStatement.setObject(1, id);
            }

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                UUID instanceId = resultSet.getObject(collectionMetadata.getLinkingColumn(), UUID.class);
                UniqueData instance = results.get(instanceId);

                if (instance == null) {
                    instance = uniqueDataMetadata.createInstance(this);
                    uniqueDataMetadata.setIdentifyingValue(instance, instanceId);
                    results.put(instanceId, instance);
                }

                PersistentCollection<?> collection = collectionMetadata.getCollection(instance);
                try {
                    CollectionEntry entry = collectionMetadata.createEntry(this);
                    List<PersistentEntryValueMetadata> entryValueMetadata = collectionMetadata.getEntryValueMetadata();
                    for (PersistentEntryValueMetadata valueMetadata : entryValueMetadata) {
                        PersistentEntryValue<?> entryValue = valueMetadata.getValue(entry);
                        Object value = resultSet.getObject(valueMetadata.getColumn());
                        Object deserializedValue = deserialize(entryValue.getType(), value);
                        entryValue.setInitialValue(deserializedValue);
                    }
                    collection.addAllInternal(Collections.singleton(entry));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //Collections end

        //CachedValues start
        List<CachedValueMetadata> cachedValueMetadataList = uniqueDataMetadata.getValueMetadata(CachedValueMetadata.class);

        if (!cachedValueMetadataList.isEmpty()) {
            for (UniqueData instance : results.values()) {
                for (CachedValueMetadata valueMetadata : cachedValueMetadataList) {
                    CachedValue<?> cachedValue = valueMetadata.getSharedValue(instance);
                    String key = buildCachedValueKey(valueMetadata.getKey(), instance);
                    String encoded = jedis.get(key);
                    if (encoded != null) {
                        Object deserialized = DatabaseSupportedType.decode(encoded);
                        Object value = deserialize(valueMetadata.getType(), deserialized);
                        cachedValue.setInternal(value);
                    }
                }
            }
        }
        //CachedValues end

        return results.values();
    }

    private String buildSelectValuesStatement(UniqueDataMetadata metadata, boolean byId) {
        StringBuilder sb = new StringBuilder("SELECT ");

        List<PersistentValueMetadata> persistentValueMetadataList = metadata.getValueMetadata(PersistentValueMetadata.class);
        List<ForeignPersistentValueMetadata> foreignPersistentValueMetadataList = metadata.getValueMetadata(ForeignPersistentValueMetadata.class);

        List<String> columns = new ArrayList<>();

        columns.add(metadata.getTopLevelTable() + ".id");

        for (PersistentValueMetadata valueMetadata : persistentValueMetadataList) {
            columns.add(valueMetadata.getTable() + "." + valueMetadata.getColumn());
        }

        for (int i = 0; i < columns.size(); i++) {
            sb.append(columns.get(i));
            if (i < columns.size() - 1) {
                sb.append(", ");
            }
        }

        List<String> foreignColumns = new ArrayList<>();
        List<String> foreignObjectIdColumns = new ArrayList<>();

        for (ForeignPersistentValueMetadata valueMetadata : foreignPersistentValueMetadataList) {
            foreignColumns.add(valueMetadata.getTable() + "." + valueMetadata.getColumn());

            String foreignObjectIdColumn = valueMetadata.getLinkingTable() + "." + valueMetadata.getForeignLinkingColumn();
            if (foreignObjectIdColumns.contains(foreignObjectIdColumn)) {
                continue;
            }
            foreignObjectIdColumns.add(foreignObjectIdColumn);
        }

        if (!foreignColumns.isEmpty()) {
            sb.append(", ");
        }

        for (int i = 0; i < foreignColumns.size(); i++) {
            sb.append(foreignColumns.get(i));
            if (i < foreignColumns.size() - 1) {
                sb.append(", ");
            }
        }

        if (!foreignObjectIdColumns.isEmpty()) {
            sb.append(", ");
        }

        for (int i = 0; i < foreignObjectIdColumns.size(); i++) {
            sb.append(foreignObjectIdColumns.get(i))
                    .append(" AS ")
                    .append(foreignObjectIdColumns.get(i).replace(".", "_"));
            if (i < foreignObjectIdColumns.size() - 1) {
                sb.append(", ");
            }
        }


        sb.append(" FROM ");

        List<String> tables = new ArrayList<>(metadata.getTables());
        tables.remove(metadata.getTopLevelTable());

        sb.append(metadata.getTopLevelTable());

        for (String table : tables) {
            sb.append(" JOIN ").append(table).append(" ON ").append(metadata.getTopLevelTable()).append(".id = ").append(table).append(".id");
        }

        List<ForeignPersistentValueMetadata> joinedForeignValues = new ArrayList<>();

        for (ForeignPersistentValueMetadata valueMetadata : foreignPersistentValueMetadataList) {
            if (joinedForeignValues.stream().anyMatch(otherValueMetaData ->
                    otherValueMetaData.getLinkingTable().equals(valueMetadata.getLinkingTable()) &&
                            otherValueMetaData.getForeignLinkingColumn().equals(valueMetadata.getForeignLinkingColumn()) &&
                            otherValueMetaData.getThisLinkingColumn().equals(valueMetadata.getThisLinkingColumn())
            )) {
                continue;
            }

            sb.append(" LEFT JOIN ")
                    .append(valueMetadata.getLinkingTable())
                    .append(" ON ")
                    .append(metadata.getTopLevelTable())
                    .append(".id")
                    .append(" = ")
                    .append(valueMetadata.getLinkingTable())
                    .append(".")
                    .append(valueMetadata.getThisLinkingColumn())
                    .append(" LEFT JOIN ")
                    .append(valueMetadata.getTable())
                    .append(" ON ")
                    .append(valueMetadata.getLinkingTable())
                    .append(".")
                    .append(valueMetadata.getForeignLinkingColumn())
                    .append(" = ")
                    .append(valueMetadata.getTable())
                    .append(".id");

            joinedForeignValues.add(valueMetadata);
        }


        if (byId) {
            sb.append(" WHERE ").append(metadata.getTopLevelTable()).append(".id = ?");
        }

        return sb.toString();
    }

    private List<Pair<PersistentCollectionMetadata, String>> buildSelectCollectionsStatements(UniqueDataMetadata metadata, boolean byId) {
        List<Pair<PersistentCollectionMetadata, String>> statements = new ArrayList<>();
        List<PersistentCollectionMetadata> persistentCollectionMetadataList = metadata.getCollectionMetadata(PersistentCollectionMetadata.class);

        for (PersistentCollectionMetadata collectionMetadata : persistentCollectionMetadataList) {
            List<PersistentEntryValueMetadata> entryValueMetadata = collectionMetadata.getEntryValueMetadata();

            StringBuilder sb = new StringBuilder("SELECT ");

            List<String> columns = new ArrayList<>();
            columns.add(collectionMetadata.getLinkingColumn());
            columns.addAll(entryValueMetadata.stream().map(PersistentEntryValueMetadata::getColumn).toList());

            for (int i = 0; i < columns.size(); i++) {
                sb.append(columns.get(i));
                if (i < columns.size() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(" FROM ");
            sb.append(collectionMetadata.getTable());

            if (byId) {
                sb.append(" WHERE ").append(collectionMetadata.getLinkingColumn()).append(" = ?");
            }

            statements.add(Pair.of(collectionMetadata, sb.toString()));
        }

        return statements;
    }

    /**
     * Insert a new instance of an entity in the database.
     * The entity must have all of its {@link PersistentValue}s and {@link PersistentCollection}s set with non-null values.
     * <br><br>
     * Multiple insert operations will be executed, one for each group of {@link PersistentValue}, and one for each {@link PersistentCollection}.
     * {@link PersistentValue}s are grouped based off their tables.
     * Each {@link PersistentCollection} is inserted separately since each one resides in its own table.
     * {@link CachedValue}s are also inserted into the cache.
     *
     * @param entity The entity to create
     */
    @Blocking
    public <T extends UniqueData> void insert(T entity) {
        try (Connection connection = getConnection()) {
            try (Jedis jedis = getJedis()) {
                insert(connection, jedis, entity);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Insert a new instance of an entity in the database.
     * The entity must have all of its {@link PersistentValue}s and {@link PersistentCollection}s set with non-null values.
     * <br><br>
     * Multiple insert operations will be executed, one for each group of {@link PersistentValue}, and one for each {@link PersistentCollection}.
     * {@link PersistentValue}s are grouped based off their tables.
     * Each {@link PersistentCollection} is inserted separately since each one resides in its own table.
     * {@link CachedValue}s are also inserted into the cache.
     *
     * @param connection The connection to use
     * @param jedis      The jedis instance to use
     * @param entity     The entity to create
     * @throws SQLException If an error occurs
     */
    public <T extends UniqueData> void insert(Connection connection, Jedis jedis, T entity) throws SQLException {
        long start = System.currentTimeMillis();
        connection.setAutoCommit(false);
        UUID id = entity.getId();
        if (id == null) {
            throw new IllegalArgumentException("Entity ID is null! All entities must have a non-null ID before inserting!");
        }
        UniqueDataMetadata uniqueDataMetadata = getUniqueDataMetadata(entity.getClass());

        List<PersistentValueMetadata> persistentValueMetadataList = uniqueDataMetadata.getValueMetadata(PersistentValueMetadata.class);
        List<ForeignPersistentValueMetadata> foreignPersistentValueMetadataList = uniqueDataMetadata.getValueMetadata(ForeignPersistentValueMetadata.class);
        List<PersistentCollectionMetadata> persistentCollectionMetadataList = uniqueDataMetadata.getCollectionMetadata(PersistentCollectionMetadata.class);

        List<CachedValueMetadata> cachedValueMetadataList = uniqueDataMetadata.getValueMetadata(CachedValueMetadata.class);

        if (persistentValueMetadataList.isEmpty()) {
            throw new IllegalArgumentException("No PersistentValues found for " + entity.getClass().getName());
        }

        for (PersistentValueMetadata valueMetadata : persistentValueMetadataList) {
            PersistentValue<?> value = valueMetadata.getSharedValue(entity);
            if (value.get() == null) {
                throw new IllegalArgumentException("Value " + valueMetadata.getColumn() + " is null! All values must be non-null before inserting!");
            }
        }

        for (ForeignPersistentValueMetadata valueMetadata : foreignPersistentValueMetadataList) {
            ForeignPersistentValue<?> value = valueMetadata.getSharedValue(entity);
            if (value.get() != null) {
                throw new IllegalArgumentException("Foreign value " + valueMetadata.getColumn() + " is non-null! All foreign values should be null before insertion!");
            }
        }

        Map<String, List<PersistentValueMetadata>> tablesToMetadata = new HashMap<>();
        for (PersistentValueMetadata valueMetadata : persistentValueMetadataList) {
            String table = valueMetadata.getTable();
            tablesToMetadata.computeIfAbsent(table, k -> new ArrayList<>()).add(valueMetadata);
        }

        //Insert data into all tables, but if data is already present for that table, do NOT insert it.
        for (Map.Entry<String, List<PersistentValueMetadata>> entry : tablesToMetadata.entrySet()) {
            StringBuilder sb = new StringBuilder("INSERT INTO ");
            sb.append(entry.getKey()).append(" (");

            List<String> columns = new ArrayList<>();
            columns.add("id");
            for (PersistentValueMetadata valueMetadata : entry.getValue()) {
                columns.add(valueMetadata.getColumn());
            }

            for (int i = 0; i < columns.size(); i++) {
                sb.append(columns.get(i));
                if (i < columns.size() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(") SELECT ");

            for (int i = 0; i < columns.size(); i++) {
                sb.append("?");
                if (i < columns.size() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(" WHERE NOT EXISTS (SELECT 1 FROM ").append(entry.getKey()).append(" WHERE id = ?)");

            String sql = sb.toString();
            debug("Executing update: " + sql);

            PreparedStatement statement = connection.prepareStatement(sql);

            statement.setObject(1, id);
            for (int i = 1; i < columns.size(); i++) {
                PersistentValue<?> value = entry.getValue().get(i - 1).getSharedValue(entity);
                Object serialized = serialize(value.get());
                statement.setObject(i + 1, serialized);
            }

            statement.setObject(columns.size() + 1, id);

            statement.execute();
        }

        for (PersistentCollectionMetadata collectionMetadata : persistentCollectionMetadataList) {
            PersistentCollection<?> collection = collectionMetadata.getCollection(entity);

            if (!collection.isEmpty()) {
                collection.addInternalValuesToDataSource(connection);
            }
        }

        connection.setAutoCommit(true);

        //CachedValues start
        List<Pair<String, String>> initializedCachedValues = new ArrayList<>();

        for (CachedValueMetadata valueMetadata : cachedValueMetadataList) {
            CachedValue<?> cachedValue = valueMetadata.getSharedValue(entity);
            Object value = cachedValue.get();
            if (Objects.equals(value, cachedValue.getDefaultValue())) {
                continue;
            }

            String key = buildCachedValueKey(valueMetadata.getKey(), entity);
            Object serialized = serialize(value);
            String encoded = DatabaseSupportedType.encode(serialized);
            initializedCachedValues.add(Pair.of(key, encoded));
        }

        if (!initializedCachedValues.isEmpty()) {
            Pipeline pipeline = jedis.pipelined();
            for (Pair<String, String> pair : initializedCachedValues) {
                pipeline.set(pair.first(), pair.second());
            }
            pipeline.sync();
        }
        //CachedValues end

        getMessenger().broadcastMessageNoPrefix(
                getChannel(entity),
                DataInsertMessageHandler.class,
                new DataLookupMessage(
                        uniqueDataMetadata.getTopLevelTable(),
                        new ArrayList<>(uniqueDataMetadata.getTables()),
                        entity.getId()
                ));


        //Since the values on some tables might differ, get the state from the db and update the instance
        long startGetState = System.currentTimeMillis();
        UniqueData fromDatasource = select(connection, jedis, uniqueDataMetadata, true, id).iterator().next();
        copyValues(fromDatasource, entity);

        long end = System.currentTimeMillis();
        debug("Insert took " + (end - start) + "ms. Took " + (end - startGetState) + "ms to get state from the datasource.");
    }

    /**
     * Delete an entity from the database.
     *
     * @param data         The entity to delete
     * @param deletionType The type of deletion to perform
     */
    public void delete(UniqueData data, DeletionType deletionType) {
        try (Connection connection = getConnection()) {
            try (Jedis jedis = getJedis()) {
                delete(connection, jedis, data, deletionType);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete an entity from the database.
     *
     * @param connection   The connection to use
     * @param jedis        The jedis instance to use
     * @param data         The entity to delete
     * @param deletionType The type of deletion to perform
     * @throws SQLException If an error occurs
     */
    public void delete(Connection connection, Jedis jedis, UniqueData data, DeletionType deletionType) throws SQLException {
        //Currently we only support fully deleting an entity as other types of deletions are much more complicated and not needed as of now.
        if (deletionType == DeletionType.ALL) {
            deleteAll(connection, jedis, data);
        }
    }

    private void deleteAll(Connection connection, Jedis jedis, UniqueData data) throws SQLException {
        UniqueDataMetadata uniqueDataMetadata = getUniqueDataMetadata(data.getClass());

        List<PersistentValueMetadata> persistentValueMetadataList = uniqueDataMetadata.getValueMetadata(PersistentValueMetadata.class);
        List<ForeignPersistentValueMetadata> foreignPersistentValueMetadataList = uniqueDataMetadata.getValueMetadata(ForeignPersistentValueMetadata.class);
        List<PersistentCollectionMetadata> persistentCollectionMetadataList = uniqueDataMetadata.getCollectionMetadata(PersistentCollectionMetadata.class);

        Map<String, List<PersistentValueMetadata>> tablesToMetadata = new HashMap<>();

        for (PersistentValueMetadata valueMetadata : persistentValueMetadataList) {
            String table = valueMetadata.getTable();
            tablesToMetadata.computeIfAbsent(table, k -> new ArrayList<>()).add(valueMetadata);
        }

        connection.setAutoCommit(false);

        for (Map.Entry<String, List<PersistentValueMetadata>> entry : tablesToMetadata.entrySet()) {
            String table = entry.getKey();
            String sql = "DELETE FROM " + table + " WHERE id = ?";
            debug("Executing update: " + sql);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setObject(1, data.getId());

            statement.execute();
        }

        for (PersistentCollectionMetadata collectionMetadata : persistentCollectionMetadataList) {
            String table = collectionMetadata.getTable();
            String sql = "DELETE FROM " + table + " WHERE " + collectionMetadata.getLinkingColumn() + " = ?";
            debug("Executing update: " + sql);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setObject(1, data.getId());

            statement.execute();
        }

        connection.setAutoCommit(true);

        List<CachedValueMetadata> cachedValueMetadataList = uniqueDataMetadata.getValueMetadata(CachedValueMetadata.class);
        if (!cachedValueMetadataList.isEmpty()) {
            Pipeline pipeline = jedis.pipelined();
            for (CachedValueMetadata valueMetadata : cachedValueMetadataList) {
                String key = buildCachedValueKey(valueMetadata.getKey(), data);
                pipeline.del(key);
            }
            pipeline.sync();
        }

        getMessenger().broadcastMessageNoPrefix(
                getChannel(data),
                DataDeleteAllMessageHandler.class,
                new DataLookupMessage(
                        uniqueDataMetadata.getTopLevelTable(),
                        new ArrayList<>(uniqueDataMetadata.getTables()),
                        data.getId()
                )
        );
    }

    /**
     * Register a serializer for a custom object type.
     *
     * @param serializer The serializer to register
     */
    public void registerSerializer(ValueSerializer<?, ?> serializer) {
        if (DatabaseSupportedType.isSupported(serializer.getDeserializedType())) {
            throw new IllegalArgumentException("%s could not be registered since its deserialized type is already supported by the database!".formatted(serializer.getClass().getSimpleName()));
        }

        if (!DatabaseSupportedType.isSupported(serializer.getSerializedType())) {
            throw new IllegalArgumentException("%s could not be registered since its serialized type is not supported by the database!".formatted(serializer.getClass().getSimpleName()));
        }

        for (ValueSerializer<?, ?> s : SERIALIZERS) {
            if (s.getDeserializedType().isAssignableFrom(serializer.getDeserializedType())) {
                throw new IllegalArgumentException("A serializer for " + serializer.getDeserializedType().getName() + " is already registered!");
            }
        }

        SERIALIZERS.add(serializer);
    }

    /**
     * Attempt to serialize an object into something that can be stored in the database.
     *
     * @param value The value to serialize
     * @return The serialized value
     */
    public Object serialize(@NotNull Object value) {
        if (DatabaseSupportedType.isSupported(value.getClass())) {
            return value;
        }

        ValueSerializer<?, ?> serializer = null;

        for (ValueSerializer<?, ?> s : SERIALIZERS) {
            if (s.getDeserializedType().isAssignableFrom(value.getClass())) {
                serializer = s;
                break;
            }
        }

        if (serializer == null) {
            throw new IllegalArgumentException("No serializer found for " + value.getClass().getName());
        }

        Object serialized = serializer.serialize(value);

        if (!DatabaseSupportedType.isSupported(serialized.getClass())) {
            throw new IllegalArgumentException("Serializer for " + value.getClass().getName() + " returned an unsupported type: " + serialized.getClass().getName());
        }

        return serialized;
    }

    /**
     * Attempt to deserialize a value from the database into its original form.
     *
     * @param deserializedType The type of the deserialized value
     * @param serialized       The value to deserialize
     * @return The deserialized value
     */
    public Object deserialize(Class<?> deserializedType, Object serialized) {
        if (serialized.getClass().equals(deserializedType)) {
            return serialized;
        }

        //Attempt to deserialize it if, for example, an integer is supposed to become a short.
        Object deserialized = DatabaseSupportedType.deserialize(serialized, deserializedType);
        if (deserialized != null) {
            return deserialized;
        }


        ValueSerializer<?, ?> serializer = null;

        for (ValueSerializer<?, ?> s : SERIALIZERS) {
            if (s.getDeserializedType().isAssignableFrom(deserializedType)) {
                serializer = s;
                break;
            }
        }

        if (serializer == null) {
            throw new IllegalArgumentException("No serializer found for " + deserializedType.getName() + "! Serialized: " + serialized.getClass().getName());
        }

        if (!serializer.getSerializedType().equals(serialized.getClass())) {
            throw new IllegalArgumentException("Serializer %s expected to deserialize a(n) %s, but got a(n) %s instead!".formatted(
                    serializer.getClass().getSimpleName(),
                    serializer.getSerializedType().getSimpleName(),
                    serialized.getClass().getSimpleName()
            ));
        }

        return serializer.deserialize(serialized);
    }

    private void copyValues(UniqueData source, UniqueData destination) {
        if (!source.getClass().equals(destination.getClass())) {
            throw new IllegalArgumentException("Source and destination must be of the same class!");
        }

        UniqueDataMetadata metadata = getUniqueDataMetadata(source.getClass());

        List<AbstractPersistentValueMetadata<?>> abstractPersistentValueMetadataList = getAbstractPersistentValueMetadata(metadata);

        for (AbstractPersistentValueMetadata<?> valueMetadata : abstractPersistentValueMetadataList) {
            AbstractPersistentValue<?, ?> sourceValue = (AbstractPersistentValue<?, ?>) valueMetadata.getSharedValue(source);
            AbstractPersistentValue<?, ?> destinationValue = (AbstractPersistentValue<?, ?>) valueMetadata.getSharedValue(destination);
            destinationValue.setInternal(sourceValue.get());
        }
    }

    public String buildCachedValueKey(String key, String table, UUID dataId) {
        //We should probably factor in the current schema from the hikari config as well
        return "cached_value:" + table + ":" + dataId + ":" + key;
    }

    public String buildCachedValueKey(String key, UniqueData data) {
        UniqueDataMetadata metadata = getUniqueDataMetadata(data.getClass());
        String table = metadata.getTable(data.getClass());

        return buildCachedValueKey(key, table, data.getId());
    }


    @Blocking
    public void linkForeignObjects(Connection connection, ForeignPersistentValue<?> fpv, UUID otherId) throws SQLException, ForeignReferenceDoesNotExistException {
        UUID prevForeignId = fpv.getForeignObjectId();
        Object foreignDataValue = null;
        if (otherId == null) {
            String removeObjectReferenceSql = "DELETE FROM " + fpv.getLinkingTable() + " WHERE " + fpv.getThisLinkingColumn() + " = ? AND " + fpv.getForeignLinkingColumn() + " = ?";
            debug("Executing update: " + removeObjectReferenceSql);

            PreparedStatement removeObjectReferenceStatement = connection.prepareStatement(removeObjectReferenceSql);
            removeObjectReferenceStatement.setObject(1, fpv.getParent().getId());
            removeObjectReferenceStatement.setObject(2, fpv.getForeignObjectId());

            removeObjectReferenceStatement.execute();
        } else {
            String getOtherObjectSql = "SELECT id FROM " + fpv.getTable() + " WHERE id = ?";
            debug("Executing query: " + getOtherObjectSql);

            PreparedStatement getOtherObjectStatement = connection.prepareStatement(getOtherObjectSql);


            getOtherObjectStatement.setObject(1, otherId);
            if (!getOtherObjectStatement.executeQuery().next()) {
                throw new ForeignReferenceDoesNotExistException("Object with id doesnt exist! Id: " + otherId);
            }

            String updateLinkingTableSql = "INSERT INTO " + fpv.getLinkingTable() + " (" + fpv.getThisLinkingColumn() + ", " + fpv.getForeignLinkingColumn() + ") VALUES (?, ?) ON CONFLICT (" + fpv.getThisLinkingColumn() + ", " + fpv.getForeignLinkingColumn() + ") DO UPDATE SET " + fpv.getThisLinkingColumn() + " = EXCLUDED." + fpv.getThisLinkingColumn();

            debug("Executing update: " + updateLinkingTableSql);

            PreparedStatement statement = connection.prepareStatement(updateLinkingTableSql);
            statement.setObject(1, fpv.getParent().getId());
            statement.setObject(2, otherId);
            statement.execute();

            foreignDataValue = getForeignObjectValue(connection, fpv, otherId);
        }

        linkLocalForeignPersistentValues(connection,
                fpv.getColumn(),
                fpv.getLinkingTable(),
                fpv.getThisLinkingColumn(),
                fpv.getForeignLinkingColumn(),
                fpv.getParent().getId(),
                otherId,
                prevForeignId,
                foreignDataValue
        );

        getMessenger().broadcastMessageNoPrefix(
                getForeignLinkChannel(fpv.getMetadata()),
                ForeignLinkUpdateMessageHandler.class,
                new ForeignLinkUpdateMessage(
                        fpv.getColumn(),
                        fpv.getLinkingTable(),
                        fpv.getParent().getId(),
                        otherId,
                        prevForeignId,
                        fpv.getThisLinkingColumn(),
                        fpv.getForeignLinkingColumn(),
                        foreignDataValue
                ));
    }

    @ApiStatus.Internal
    public void linkLocalForeignPersistentValues(
            Connection connection,
            String column,
            String linkingTable,
            String linkingColumn1,
            String linkingColumn2,
            UUID id1,
            UUID id2,
            UUID prevForeignId,
            Object value2
    ) throws SQLException {
        Collection<ForeignPersistentValueMetadata> fpvMetaList = getForeignLinks(linkingTable);

        if (fpvMetaList.isEmpty()) {
            logger.warn("Could not find metadata for linking table: {}", linkingTable);
            return;
        }

        for (ForeignPersistentValueMetadata fpvMeta : fpvMetaList) {
            UniqueDataMetadata uniqueDataMetadata = getUniqueDataMetadata(fpvMeta.getParentClass());

            if (!fpvMeta.getLinkingTable().equals(linkingTable)) {
                continue;
            }

            if (!fpvMeta.getThisLinkingColumn().equals(linkingColumn1) && !fpvMeta.getThisLinkingColumn().equals(linkingColumn2)) {
                continue;
            }

            if (!fpvMeta.getForeignLinkingColumn().equals(linkingColumn1) && !fpvMeta.getForeignLinkingColumn().equals(linkingColumn2)) {
                continue;
            }

            UUID dataId;
            UUID foreignId;
            Object foreignDataValue = null;

            if (linkingColumn1.equals(fpvMeta.getThisLinkingColumn())) {
                dataId = id1;
                foreignId = id2;
                foreignDataValue = value2;
            } else {
                dataId = id2 == null ? prevForeignId : id2; //Will be null if we unlink
                foreignId = id1;
            }

            UniqueData data = uniqueDataMetadata.getProvider().get(dataId);

            if (data == null) {
                logger.warn("Data with id {} does not exist!", dataId);
                continue;
            }

            ForeignPersistentValue<?> fpv = fpvMeta.getSharedValue(data);

            assert fpv != null;

            if (id2 == null) {
                //We are unlinking
                foreignDataValue = null;
                foreignId = null;
            } else if (!(fpv.getColumn().equals(column) && linkingColumn1.equals(fpvMeta.getThisLinkingColumn()))) {
                //The FPV is different from the one linked so we have to get the value, we can't just use the cached value (value2)
                foreignDataValue = getForeignObjectValue(connection, fpv, foreignId);

                if (prevForeignId != null && linkingColumn2.equals(fpvMeta.getThisLinkingColumn())) {
                    //We are switching foreign objects, and we need to unlink the previous object
                    UniqueData prevData = uniqueDataMetadata.getProvider().get(prevForeignId);

                    if (prevData == null) {
                        logger.warn("(Previous) Data with id {} does not exist!", prevForeignId);
                        continue;
                    }

                    ForeignPersistentValue<?> prevFpv = fpvMeta.getSharedValue(prevData);
                    prevFpv.setInternalForeignObject(null, null);
                }
            }

            fpv.setInternalForeignObject(foreignId, foreignDataValue);
        }
    }

    public <T> T getForeignObjectValue(Connection connection, ForeignPersistentValue<T> fpv, @NotNull UUID otherId) throws SQLException {
        String getColumnValueSql = "SELECT " + fpv.getColumn() + " FROM " + fpv.getTable() + " WHERE id = ?";
        debug("Executing query: " + getColumnValueSql);

        PreparedStatement getColumnValueStatement = connection.prepareStatement(getColumnValueSql);
        getColumnValueStatement.setObject(1, otherId);
        ResultSet resultSet = getColumnValueStatement.executeQuery();
        resultSet.next();
        Object value = resultSet.getObject(fpv.getColumn());
        return (T) deserialize(fpv.getType(), value);
    }

}
