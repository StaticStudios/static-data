package net.staticstudios.data.meta;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;
import net.staticstudios.data.Table;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.shared.SharedCollection;
import net.staticstudios.data.shared.SharedValue;
import net.staticstudios.data.value.PersistentCollection;
import net.staticstudios.data.value.PersistentValue;
import net.staticstudios.utils.Wrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public class UniqueDataMetadata {

    private final DataManager dataManager;
    private final Class<? extends UniqueData> type;
    private final Map<Field, Metadata> metadataMap;
    private final Map<String, Class<? extends UniqueData>> categorizedData;
    private final Field idValueField;
    private Constructor<?> constructor;
    private String topLevelTable;

    private UniqueDataMetadata(DataManager dataManager, Class<? extends UniqueData> type) {
        this.dataManager = dataManager;
        this.type = type;
        this.metadataMap = new HashMap<>();
        this.categorizedData = new HashMap<>();
        try {
            this.idValueField = UniqueData.class.getDeclaredField("id");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        this.idValueField.setAccessible(true);
    }

    /**
     * Extract metadata about a {@link UniqueData} class.
     * This metadata will contain information about any {@link PersistentValue}s and {@link PersistentCollection}s
     *
     * @param dataManager The data manager to use
     * @param clazz       The class to extract metadata from
     * @return The metadata for the class
     */
    public static <T extends UniqueData> UniqueDataMetadata extract(DataManager dataManager, Class<T> clazz) {
        UniqueDataMetadata metadata = new UniqueDataMetadata(dataManager, clazz);
        T dummyInstance;
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            dummyInstance = constructor.newInstance();

            metadata.constructor = constructor;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create dummy instance of " + clazz.getName() + "! Make sure it has a no-args constructor!", e);
        }


        Set<SharedValueMetadata<?>> valueMetadataSet = new HashSet<>();
        Set<SharedCollectionMetadata<?, ?>> collectionMetadataSet = new HashSet<>();
        Wrapper<String> topLevelTableWrapper = new Wrapper<>();

        try {
            extractMemberMetadata(dataManager, clazz, dummyInstance, null, valueMetadataSet, collectionMetadataSet, topLevelTableWrapper, metadata.categorizedData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract information from " + clazz.getName(), e);
        }

        if (!topLevelTableWrapper.hasValue()) {
            throw new RuntimeException("Class " + clazz.getName() + " is missing a Table annotation!");
        }

        metadata.topLevelTable = topLevelTableWrapper.get();

        for (SharedValueMetadata<?> valueMetadata : valueMetadataSet) {
            metadata.metadataMap.put(valueMetadata.getField(), valueMetadata);
        }

        for (SharedCollectionMetadata<?, ?> collectionMetadata : collectionMetadataSet) {
            metadata.metadataMap.put(collectionMetadata.getField(), collectionMetadata);
        }
        return metadata;
    }

    private static void extractMemberMetadata(
            DataManager dataManager,
            Class<? extends UniqueData> clazz,
            Object dummyInstance,
            @Nullable String superTable,
            Set<SharedValueMetadata<?>> valueMetadataSet,
            Set<SharedCollectionMetadata<?, ?>> collectionMetadataSet,
            Wrapper<String> topLevelTableWrapper,
            Map<String, Class<? extends UniqueData>> categorizedData
    ) throws Exception {
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        if (superTable == null && tableAnnotation == null) {
            throw new RuntimeException("Class " + clazz.getName() + " is missing a Table annotation!");
        }

        String table;

        if (tableAnnotation != null) {
            table = tableAnnotation.value();
            categorizedData.put(table, clazz);

            if (!topLevelTableWrapper.hasValue()) {
                topLevelTableWrapper.set(table);
            }
        } else {
            table = superTable;
        }

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(dummyInstance);

            if (value instanceof SharedValue<?> sharedValue) {
                Method extractCollectionMetadata = sharedValue.getMetadataClass().getDeclaredMethod("extract", DataManager.class, Class.class, String.class, sharedValue.getClass(), Field.class);
                extractCollectionMetadata.setAccessible(true);
                SharedValueMetadata<?> sharedValueMetadata = (SharedValueMetadata<?>) extractCollectionMetadata.invoke(null, dataManager, clazz, table, sharedValue, field);

                valueMetadataSet.add(sharedValueMetadata);
            } else if (value instanceof SharedCollection<?, ?, ?> sharedCollection) {
                Method extractCollectionMetadata = sharedCollection.getMetadataClass().getDeclaredMethod("extract", DataManager.class, Class.class, sharedCollection.getClass(), Field.class);
                extractCollectionMetadata.setAccessible(true);
                SharedCollectionMetadata<?, ?> collectionMetadata = (SharedCollectionMetadata<?, ?>) extractCollectionMetadata.invoke(null, dataManager, clazz, sharedCollection, field);

                collectionMetadataSet.add(collectionMetadata);
            }
        }

        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null && superClass.isAssignableFrom(dummyInstance.getClass())) {
            extractMemberMetadata(dataManager, (Class<? extends UniqueData>) superClass, dummyInstance, table, valueMetadataSet, collectionMetadataSet, topLevelTableWrapper, categorizedData);
        }
    }

    public Class<? extends UniqueData> getType() {
        return type;
    }

    public <T extends SharedValueMetadata> List<T> getValueMetadata(Class<T> clazz) {
        List<T> values = new ArrayList<>();

        for (Metadata metadata : metadataMap.values()) {
            if (metadata.getClass().isAssignableFrom(clazz)) {
                values.add((T) metadata);
            }
        }

        return values;
    }

    public <T extends SharedCollectionMetadata<?, ?>> List<T> getCollectionMetadata(Class<T> clazz) {
        List<T> values = new ArrayList<>();

        for (Metadata metadata : metadataMap.values()) {
            if (metadata.getClass().isAssignableFrom(clazz)) {
                values.add((T) metadata);
            }
        }

        return values;
    }

    public UniqueData createInstance(DataManager dataManager) {
        try {
            UniqueData data = (UniqueData) constructor.newInstance();
            Method setDataManager = UniqueData.class.getDeclaredMethod("setDataManager", DataManager.class);
            setDataManager.setAccessible(true);
            setDataManager.invoke(data, dataManager);
            return data;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create instance of " + type.getName(), e);
        }
    }

    public @NotNull DataProvider<?> getProvider() {
        return dataManager.getDataProvider(type);
    }

    public void setIdentifyingValue(UniqueData data, UUID id) {
        try {
            idValueField.set(data, id);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends Metadata> T getMetadata(Class<T> type, Field field) {
        Object value = metadataMap.get(field);

        if (value == null) {
            throw new RuntimeException("No metadata found for field " + field.getName() + " in " + this.type.getName());
        }

        if (!type.isAssignableFrom(value.getClass())) {
            throw new RuntimeException("Metadata for field " + field.getName() + " in " + this.type.getName() + " is not of type " + type.getName());
        }

        return (T) value;
    }

    /**
     * Get a set which contains all member metadata extracted.
     * This includes {@link SharedValueMetadata} and {@link SharedCollectionMetadata}
     *
     * @return A set of all member metadata
     */
    public Set<Metadata> getMemberMetadata() {
        return new HashSet<>(metadataMap.values());
    }

    public Set<String> getTables() {
        return categorizedData.keySet();
    }

    public String getTopLevelTable() {
        return topLevelTable;
    }

    /**
     * Get the table for a class
     *
     * @param clazz The class to get the table for
     * @return The table
     */
    public @NotNull String getTable(Class<?> clazz) {
        return categorizedData.entrySet().stream()
                .filter(entry -> entry.getValue().equals(clazz))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Failed to find table for " + clazz.getName()));
    }
}
