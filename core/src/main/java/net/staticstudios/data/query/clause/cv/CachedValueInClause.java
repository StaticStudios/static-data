package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.query.clause.ValueClause;
import net.staticstudios.data.util.UniqueDataMetadata;
import net.staticstudios.data.util.redis.RedisUtils;

import java.util.ArrayList;
import java.util.List;

public class CachedValueInClause extends AbstractCachedValueClause {
    private final Object[] values;

    public CachedValueInClause(String schema, String table, String identifier, Object[] values) {
        super(schema, table, identifier);
        this.values = values;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> append(StringBuilder sb, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        sb.append("\"").append(schema).append("\".\"").append(table).append("\".\"").append(RedisUtils.getVirtualColumnName(identifier)).append("\" IN (");
        for (int i = 0; i < values.length; i++) {
            sb.append("?");
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");

        List<Object> encoded = new ArrayList<>();
        for (Object value : values) {
            encoded.add(encodeValue(value, dataManager, holderMetadata));
        }
        return encoded;
    }
}
