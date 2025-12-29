package net.staticstudios.data;

import com.google.common.base.Preconditions;
import net.staticstudios.data.insert.BatchInsert;
import net.staticstudios.data.query.QueryBuilder;
import org.jetbrains.annotations.Blocking;

/**
 * Entry point for initializing and interacting with the StaticData system.
 */
public class StaticData {
    private static boolean initialized = false;

    /**
     * Initializes the StaticData system with the provided configuration.
     *
     * @param config the configuration to use for initialization
     */
    public static void init(StaticDataConfig config) {
        initialized = true;
        new DataManager(config, true);
    }

    /**
     * Loads the specified UniqueData classes into the StaticData system.
     * Loading a class does two main things:<br>
     * 1. It extracts the class's metadata to prepare for subsequent operations. This process is done recursively, so any referenced classes will also be loaded automatically. They do not need to be specified here.<br>
     * 2. It pulls all existing records of the specified classes from the database into memory, making them readily accessible for future operations.
     *
     * @param classes the UniqueData classes to load
     */
    @SafeVarargs
    public static void load(Class<? extends UniqueData>... classes) {
        assertInit();
        DataManager.getInstance().load(classes);
    }

    /**
     * Completes the loading process by ensuring all data is fully loaded and ready for use.
     * This method should be called after invoking the {@link #load(Class[])} method.
     */
    public static void finishLoading() {
        assertInit();
        DataManager.getInstance().finishLoading();
    }

    /**
     * Create an BatchInsert for batching multiple insert operations together.
     *
     * @return a new BatchInsert instance
     */
    public static BatchInsert createBatchInsert() {
        assertInit();
        return DataManager.getInstance().createBatchInsert();
    }

    private static void assertInit() {
        Preconditions.checkState(initialized, "StaticData has not been initialized! Please call StaticData.init(...) before using any other methods.");
    }

    /**
     * Creates a snapshot of the given UniqueData instance.
     * The snapshot instance will have the same ID columns as the original instance,
     * but it's values will be read-only and represent the state of the data at the time of snapshot creation.
     *
     * @param instance the UniqueData instance to create a snapshot of
     * @param <T>      the type of UniqueData
     * @return a snapshot UniqueData instance
     */
    public static <T extends UniqueData> T createSnapshot(T instance) {
        assertInit();
        return DataManager.getInstance().createSnapshot(instance);
    }

    public static void registerValueSerializer(ValueSerializer<?, ?> serializer) {
        assertInit();
        DataManager.getInstance().registerValueSerializer(serializer);
    }

    public static <U extends UniqueData> QueryBuilder<U> query(Class<U> type) {
        assertInit();
        return new QueryBuilder<>(DataManager.getInstance(), type);
    }

    /**
     * Block the calling thread until all previously enqueued tasks have been completed
     */
    @Blocking
    public static void flushTaskQueue() {
        assertInit();
        DataManager.getInstance().flushTaskQueue();
    }
}
