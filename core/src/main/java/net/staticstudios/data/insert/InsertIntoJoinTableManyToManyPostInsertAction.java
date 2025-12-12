package net.staticstudios.data.insert;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.data.PersistentManyToManyCollectionImpl;
import net.staticstudios.data.util.*;

import java.util.ArrayList;
import java.util.List;

public class InsertIntoJoinTableManyToManyPostInsertAction<T extends UniqueData> implements PostInsertAction {
    private final DataManager dataManager;
    private final PersistentManyToManyCollectionMetadata collectionMetadata;
    private final List<Object> values;


    public InsertIntoJoinTableManyToManyPostInsertAction(DataManager dataManager, PersistentManyToManyCollectionMetadata collectionMetadata, Class<? extends UniqueData> referringClass, Class<? extends UniqueData> referencedClass, List<ColumnValuePair> referringIds, List<ColumnValuePair> referencedIds) {
        this.dataManager = dataManager;
        this.collectionMetadata = collectionMetadata;
        this.values = new ArrayList<>();
        UniqueDataMetadata referringMetadata = dataManager.getMetadata(referringClass);
        UniqueDataMetadata referencedMetadata = dataManager.getMetadata(referencedClass);

        for (ColumnMetadata columnMetadata : referringMetadata.idColumns()) {
            ColumnValuePair idValue = referringIds.stream()
                    .filter(idVal -> idVal.column().equals(columnMetadata.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Referring IDs must contain value for column: " + columnMetadata.name()));
            this.values.add(idValue.value());
        }

        for (ColumnMetadata columnMetadata : referencedMetadata.idColumns()) {
            ColumnValuePair idValue = referencedIds.stream()
                    .filter(idVal -> idVal.column().equals(columnMetadata.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Referenced IDs must contain value for column: " + columnMetadata.name()));
            this.values.add(idValue.value());
        }
    }

    @Override
    public List<SQlStatement> getStatements() {
        SQLTransaction.Statement update = PersistentManyToManyCollectionImpl.buildUpdateStatement(this.dataManager, this.collectionMetadata);
        return List.of(new SQlStatement(
                update.getH2Sql(),
                update.getPgSql(),
                this.values
        ));
    }

    public static class Builder {
        private final DataManager dataManager;
        private final List<ColumnValuePair> referringIds = new ArrayList<>();
        private final List<ColumnValuePair> referencedIds = new ArrayList<>();
        private Class<? extends UniqueData> referringClass;
        private Class<? extends UniqueData> referencedClass;
        private String joinTableSchema;
        private String joinTableName;

        public Builder(DataManager dataManager) {
            this.dataManager = dataManager;
        }

        public Builder referringClass(Class<? extends UniqueData> referringClass) {
            this.referringClass = referringClass;
            return this;
        }

        public Builder referencedClass(Class<? extends UniqueData> referencedClass) {
            this.referencedClass = referencedClass;
            return this;
        }

        public Builder joinTableSchema(String joinTableSchema) {
            this.joinTableSchema = ValueUtils.parseValue(joinTableSchema);
            return this;
        }

        public Builder joinTableName(String joinTableName) {
            this.joinTableName = ValueUtils.parseValue(joinTableName);
            return this;
        }

        public Builder referringId(String column, Object value) {
            this.referringIds.add(new ColumnValuePair(ValueUtils.parseValue(column), value));
            return this;
        }

        public Builder referencedId(String column, Object value) {
            this.referencedIds.add(new ColumnValuePair(ValueUtils.parseValue(column), value));
            return this;
        }

        public PostInsertAction build() {
            Preconditions.checkNotNull(dataManager, "DataManager must be provided.");
            Preconditions.checkNotNull(joinTableSchema, "Join table schema must be provided.");
            Preconditions.checkNotNull(joinTableName, "Join table name must be provided.");
            Preconditions.checkNotNull(referringClass, "Referring class must be provided.");
            Preconditions.checkNotNull(referencedClass, "Referenced class must be provided.");
            UniqueDataMetadata referringMetadata = dataManager.getMetadata(referringClass);

            PersistentManyToManyCollectionMetadata collectionMetadata = (PersistentManyToManyCollectionMetadata) referringMetadata.persistentCollectionMetadata().values().stream().filter(meta -> {
                if (!(meta instanceof PersistentManyToManyCollectionMetadata manyToManyMeta)) {
                    return false;
                }
                if (!manyToManyMeta.getReferencedType().equals(referencedClass)) {
                    return false;
                }
                String parsedJoinTableSchema = manyToManyMeta.getJoinTableSchema(dataManager);
                String parsedJoinTableName = manyToManyMeta.getJoinTableName(dataManager);
                return parsedJoinTableSchema.equals(joinTableSchema) && parsedJoinTableName.equals(joinTableName);
            }).findFirst().orElseThrow(() -> new IllegalArgumentException("No many-to-many collection found for the given parameters."));


            Preconditions.checkState(!referringIds.isEmpty(), "At least one referring ID must be provided.");
            Preconditions.checkState(!referencedIds.isEmpty(), "At least one referenced ID must be provided.");

            Preconditions.checkState(referringIds.size() == referringMetadata.idColumns().size(), "Number of referring IDs provided does not match number of ID columns in referring class.");
            Preconditions.checkState(referencedIds.size() == dataManager.getMetadata(referencedClass).idColumns().size(), "Number of referenced IDs provided does not match number of ID columns in referenced class.");

            return new InsertIntoJoinTableManyToManyPostInsertAction<>(
                    dataManager,
                    collectionMetadata,
                    referringClass,
                    referencedClass,
                    referringIds,
                    referencedIds
            );
        }
    }
}
