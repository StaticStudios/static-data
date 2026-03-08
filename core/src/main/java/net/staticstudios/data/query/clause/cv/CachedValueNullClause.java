package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.query.clause.ValueClause;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.util.redis.RedisUtils;

import java.util.List;

public class CachedValueNullClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String identifier;

    public CachedValueNullClause(String schema, String table, String identifier) {
        this.schema = schema;
        this.table = table;
        this.identifier = identifier;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getIdentifier() {
        return identifier;
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
