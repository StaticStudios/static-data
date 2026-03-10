package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.util.redis.RedisUtils;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class CachedValueEqualsClause extends AbstractCachedValueClause {
    private final Object value;

    public CachedValueEqualsClause(String schema, String table, String identifier, Object value) {
        super(schema, table, identifier);
        this.value = value;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> append(StringBuilder sb, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        @Nullable String encoded = encodeValue(value, dataManager, holderMetadata);

        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(RedisUtils.getVirtualColumnName(identifier)).append("\"");

        if (encoded == null) {
            sb.append(" IS NULL");
            return List.of();
        } else {
            sb.append(" = ?");
            return List.of(encoded);
        }
    }
}
