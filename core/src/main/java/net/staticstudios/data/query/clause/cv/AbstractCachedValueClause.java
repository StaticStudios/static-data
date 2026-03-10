package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.query.clause.ValueClause;
import net.staticstudios.data.util.CachedValueMetadata;
import net.staticstudios.data.util.UniqueDataMetadata;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public abstract class AbstractCachedValueClause implements ValueClause {
    protected final String schema;
    protected final String table;
    protected final String identifier;

    public AbstractCachedValueClause(String schema, String table, String identifier) {
        this.schema = schema;
        this.table = table;
        this.identifier = identifier;
    }

    protected @Nullable String encodeValue(Object value, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        CachedValueMetadata cachedValueMetadata = holderMetadata.cachedValueMetadata().values().stream()
                .filter(meta -> meta.identifier().equals(identifier))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No cached value metadata found for identifier: " + identifier));

        if (Objects.equals(cachedValueMetadata.fallbackValue(), value)) {
            return null;
        }

        return Primitives.encode(dataManager.serialize(value));
    }
}
