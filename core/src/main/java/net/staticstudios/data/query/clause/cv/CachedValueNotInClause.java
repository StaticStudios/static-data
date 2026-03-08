package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.query.clause.ValueClause;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.UniqueDataMetadata;

import java.util.ArrayList;
import java.util.List;

public class CachedValueNotInClause implements ValueClause {
    private final String schema;
    private final String table;
    private final String identifier;
    private final Object[] values;

    public CachedValueNotInClause(String schema, String table, String identifier, Object[] values) {
        this.schema = schema;
        this.table = table;
        this.identifier = identifier;
        this.values = values;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> append(StringBuilder sb, DataManager dataManager, UniqueDataMetadata holderMetadata) {
        sb.append("CACHED_VALUE(UUID ").append("'").append(dataManager.getApplicationId()).append("', '").append(schema).append("', '").append(table).append("', '").append(identifier).append("'");

        for (int i = 0; i < holderMetadata.idColumns().size(); i++) {
            ColumnMetadata columnMetadata = holderMetadata.idColumns().get(i);
            sb.append(", \"").append(columnMetadata.name()).append("\"");
        }

        sb.append(") NOT IN (");
        for (int i = 0; i < values.length; i++) {
            sb.append("?");
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        List<Object> encoded = new ArrayList<>();
        for (Object value : values) {
            encoded.add(Primitives.encode(dataManager.serialize(value)));
        }
        return encoded;
    }
}
