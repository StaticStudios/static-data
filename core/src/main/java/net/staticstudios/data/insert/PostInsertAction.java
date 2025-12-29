package net.staticstudios.data.insert;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.data.PersistentManyToManyCollectionImpl;
import net.staticstudios.data.util.*;

import java.util.List;

public interface PostInsertAction {

    static InsertIntoJoinTableManyToManyPostInsertAction.Builder manyToMany(DataManager dataManager) {
        return new InsertIntoJoinTableManyToManyPostInsertAction.Builder(dataManager);
    }

    static InsertIntoJoinTableManyToManyPostInsertAction.Builder manyToMany() {
        return new InsertIntoJoinTableManyToManyPostInsertAction.Builder(DataManager.getInstance());
    }

    static InsertIntoJoinTableManyToManyPostInsertAction.Builder manyToMany(PersistentCollection<? extends UniqueData> collection) {
        PersistentManyToManyCollectionImpl<?> impl = null;
        if (collection instanceof PersistentManyToManyCollectionImpl<?>) {
            impl = (PersistentManyToManyCollectionImpl<?>) collection;
        } else if (collection instanceof PersistentCollection.ProxyPersistentCollection<?> proxy) {
            if (proxy.getDelegate() instanceof PersistentManyToManyCollectionImpl<?> delegateImpl) {
                impl = delegateImpl;
            }
        }

        if (impl == null) {
            throw new IllegalArgumentException("The provided collection is not a PersistentManyToManyCollection!");
        }

        InsertIntoJoinTableManyToManyPostInsertAction.Builder builder = new InsertIntoJoinTableManyToManyPostInsertAction.Builder(impl.getHolder().getDataManager())
                .referringClass(impl.getHolder().getClass())
                .referencedClass(impl.getMetadata().getReferencedType())
                .joinTableSchema(impl.getMetadata().getJoinTableSchema(impl.getHolder().getDataManager()))
                .joinTableName(impl.getMetadata().getJoinTableName(impl.getHolder().getDataManager()));

        UniqueData holder = impl.getHolder();
        for (ColumnValuePair idColumnValuePair : holder.getIdColumns()) {
            builder.referringId(idColumnValuePair.column(), idColumnValuePair.value());
        }

        return builder;
    }

    static InsertIntoJoinTableManyToManyPostInsertAction.Builder manyToMany(Class<? extends UniqueData> holderClass, String collectionJoinTableSchema, String collectionJoinTableName) {
        collectionJoinTableSchema = ValueUtils.parseValue(collectionJoinTableSchema);
        collectionJoinTableName = ValueUtils.parseValue(collectionJoinTableName);
        DataManager dataManager = DataManager.getInstance();
        UniqueDataMetadata metadata = dataManager.getMetadata(holderClass);
        PersistentManyToManyCollectionMetadata collectionMetadata = null;
        for (PersistentCollectionMetadata persistentCollectionMetadata : metadata.persistentCollectionMetadata().values()) {
            if (persistentCollectionMetadata instanceof PersistentManyToManyCollectionMetadata manyToManyMetadata) {
                String joinTableSchema = manyToManyMetadata.getJoinTableSchema(dataManager);
                String joinTableName = manyToManyMetadata.getJoinTableName(dataManager);
                if (joinTableSchema.equals(collectionJoinTableSchema) && joinTableName.equals(collectionJoinTableName)) {
                    collectionMetadata = manyToManyMetadata;
                    break;
                }
            }
        }

        Preconditions.checkNotNull(collectionMetadata, "Could not find PersistentManyToManyCollectionMetadata for the provided class and join table!");
        return manyToMany(dataManager)
                .referringClass(holderClass)
                .referencedClass(collectionMetadata.getReferencedType())
                .joinTableSchema(collectionMetadata.getJoinTableSchema(dataManager))
                .joinTableName(collectionMetadata.getJoinTableName(dataManager));
    }

    List<SQlStatement> getStatements();

}
