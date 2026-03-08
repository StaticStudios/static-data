package net.staticstudios.data.query.clause.cv;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.query.clause.ValueClause;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.UniqueDataMetadata;

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
        sb.append("CACHED_VALUE(UUID ").append("'").append(dataManager.getApplicationId()).append("', '").append(schema).append("', '").append(table).append("', '").append(identifier).append("'");

        for (int i = 0; i < holderMetadata.idColumns().size(); i++) {
            ColumnMetadata columnMetadata = holderMetadata.idColumns().get(i);
            sb.append(", \"").append(columnMetadata.name()).append("\"");
        }

        sb.append(") IS NULL");
        return List.of();
    }
}
