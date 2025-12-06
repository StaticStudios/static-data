package net.staticstudios.data.insert;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.PersistentCollection;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.impl.data.PersistentManyToManyCollectionImpl;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.SQlStatement;

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

    List<SQlStatement> getStatements();

}
