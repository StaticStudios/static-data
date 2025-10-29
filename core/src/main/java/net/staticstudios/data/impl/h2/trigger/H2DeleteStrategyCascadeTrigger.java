package net.staticstudios.data.impl.h2.trigger;

import net.staticstudios.data.utils.Link;
import org.h2.api.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class H2DeleteStrategyCascadeTrigger implements Trigger {
    private final Logger logger = LoggerFactory.getLogger(H2DeleteStrategyCascadeTrigger.class);
    private final List<String> columnNames = new ArrayList<>();
    private List<Link> links;
    private String parentSchema;
    private String parentTable;
    private String targetSchema;
    private String targetTable;

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) throws SQLException {
        String[] parts;
        String data = triggerName.split("static_data_v3_")[1];
        parts = data.split("_", 2);
        int parentSchemaLength = Integer.parseInt(parts[0]);
        this.parentSchema = parts[1].substring(0, parentSchemaLength);
        data = parts[1].substring(parentSchemaLength + 1);
        parts = data.split("_", 2);
        int parentTableLength = Integer.parseInt(parts[0]);
        this.parentTable = parts[1].substring(0, parentTableLength);
        data = parts[1].substring(parentTableLength + 1);
        parts = data.split("_", 2);
        int targetSchemaLength = Integer.parseInt(parts[0]);
        this.targetSchema = parts[1].substring(0, targetSchemaLength);
        data = parts[1].substring(targetSchemaLength + 1);
        parts = data.split("_", 2);
        int targetTableLength = Integer.parseInt(parts[0]);
        this.targetTable = parts[1].substring(0, targetTableLength);
        data = parts[1].substring(targetTableLength);

        String encodedLinks = data.split("__delete_links__")[1];
        int linkCount = Integer.parseInt(encodedLinks.split("_", 2)[0]);
        encodedLinks = encodedLinks.split("_", 2)[1];
        List<String> links = new ArrayList<>();

        while (links.size() < linkCount) {
            String[] encodedParts = encodedLinks.split("_", 2);
            int length = Integer.parseInt(encodedParts[0]);
            String link = encodedParts[1].substring(0, length);
            links.add(link);
            encodedLinks = encodedParts[1].substring(length);
        }
        this.links = new ArrayList<>();
        for (int i = 0; i < links.size(); i += 2) {
            this.links.add(new Link(links.get(i + 1), links.get(i)));
        }
    }

    @Override
    public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
        int dataLength = oldRow != null ? oldRow.length : (newRow != null ? newRow.length : 0);
        if (columnNames.size() != dataLength) {
            List<String> columns = new ArrayList<>(dataLength);
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
            )) {
                ps.setString(1, parentSchema);
                ps.setString(2, parentTable); // H2 stores names in uppercase by default
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }
            columnNames.clear();
            columnNames.addAll(columns);
        }
        if (newRow == null && oldRow != null) {
            handleDelete(connection, oldRow);
        }
    }

    private void handleDelete(Connection connection, Object[] oldRow) throws SQLException {
        StringBuilder sb = new StringBuilder("DELETE FROM \"").append(targetSchema).append("\".\"").append(targetTable).append("\" WHERE ");
        List<Object> values = new ArrayList<>();
        for (Link link : links) {
            sb.append("\"").append(link.columnInReferencedTable()).append("\" = ? AND ");
            int index = columnNames.indexOf(link.columnInReferringTable());
            values.add(oldRow[index]);
        }
        sb.setLength(sb.length() - 5);
        sb.append(";");
        logger.debug("Executing cascade delete: {}", sb);

        try (PreparedStatement ps = connection.prepareStatement(sb.toString())) {
            for (int i = 0; i < values.size(); i++) {
                ps.setObject(i + 1, values.get(i));
            }
            ps.executeUpdate();
        }
    }
}
