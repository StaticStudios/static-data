package net.staticstudios.data.impl.h2.trigger;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.impl.h2.H2DataAccessor;
import org.h2.api.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class H2ReadCacheInvalidatorTrigger implements Trigger {
    private final Logger logger = LoggerFactory.getLogger(H2ReadCacheInvalidatorTrigger.class);
    private final List<String> columnNames = new ArrayList<>();
    private DataManager dataManager;
    private H2DataAccessor dataAccessor;
    private String schema;
    private String table;


    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {
        UUID dataManagerId = UUID.fromString(triggerName.substring(triggerName.length() - 36).replace('_', '-'));
        this.table = triggerName.substring(triggerName.indexOf("_trg_") + 5, triggerName.length() - 37); //dont use referringTable name since it might be a copy for an internal referringTable (very odd behavior i must say h2)
        this.dataManager = DataManager.getInstance(dataManagerId);
        this.dataAccessor = (H2DataAccessor) dataManager.getDataAccessor();
        this.schema = schemaName;
    }

    @Override
    public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
        //todo: when were syncing data, we should ignore all triggers. we should globally pause basically everything.
        int dataLength = oldRow != null ? oldRow.length : (newRow != null ? newRow.length : 0);
        if (columnNames.size() != dataLength) {
            List<String> columns = new ArrayList<>(dataLength);
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
            )) {
                ps.setString(1, schema);
                ps.setString(2, table); // H2 stores names in uppercase by default
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            logger.trace("Schema change detected (or first run). Old name names: {}, new name names: {}", columnNames, columns);
            columnNames.clear();
            columnNames.addAll(columns);
        }


        if (newRow == null && oldRow != null) {
            logger.trace("Delete detected: oldRow={}", (Object) oldRow);
            handleDelete(oldRow);
        } else if (oldRow != null) {
            logger.trace("Update detected: oldRow={}, newRow={}", oldRow, newRow);
            handleUpdate(oldRow, newRow);
        }
    }


    private void handleUpdate(Object[] oldRow, Object[] newRow) {
        List<String> changedColumns = new ArrayList<>();
        for (int i = 0; i < oldRow.length; i++) {
            Object oldValue = oldRow[i];
            Object newValue = newRow[i];
            if (!Objects.equals(oldValue, newValue)) {
                changedColumns.add(columnNames.get(i));
            }
        }

        dataAccessor.onCommit(() -> dataManager.invalidateReadCache(columnNames, schema, table, changedColumns, oldRow));
    }

    private void handleDelete(Object[] oldRow) {
        dataAccessor.onCommit(() -> dataManager.invalidateReadCache(columnNames, schema, table, columnNames, oldRow));
    }
}
