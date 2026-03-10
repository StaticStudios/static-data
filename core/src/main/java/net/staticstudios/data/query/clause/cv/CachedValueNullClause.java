package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.util.redis.RedisUtils;

import java.util.List;

public class CachedValueNullClause extends AbstractCachedValueClause {
    public CachedValueNullClause(String schema, String table, String identifier) {
        super(schema, table, identifier);
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> append(StringBuilder sb, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(RedisUtils.getVirtualColumnName(identifier)).append("\" IS NULL");
        return List.of();
    }
}
