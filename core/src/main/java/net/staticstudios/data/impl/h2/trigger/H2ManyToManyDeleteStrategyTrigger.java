package net.staticstudios.data.impl.h2.trigger;

import net.staticstudios.data.DeleteStrategy;
import net.staticstudios.data.utils.Link;
import org.h2.api.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class H2ManyToManyDeleteStrategyTrigger implements Trigger {
    private static final String TRIGGER_PREFIX = "static_data_v3_m2m_";
    private static final Map<String, Configuration> CONFIGURATIONS = new ConcurrentHashMap<>();
    private final Logger logger = LoggerFactory.getLogger(H2ManyToManyDeleteStrategyTrigger.class);
    private final List<String> holderColumnNames = new ArrayList<>();
    private String holderSchema;
    private String holderTable;
    private String joinSchema;
    private String joinTable;
    private String targetSchema;
    private String targetTable;
    private DeleteStrategy deleteStrategy;
    private List<Link> joinTableToHolderLinks;
    private List<Link> joinTableToTargetLinks;

    @Override
    public void init(Connection connection, String schemaName, String triggerName, String tableName, boolean before, int type) {
        int prefixIndex = triggerName.lastIndexOf(TRIGGER_PREFIX);
        if (prefixIndex < 0) {
            throw new IllegalArgumentException("Invalid many-to-many delete trigger name: " + triggerName);
        }
        String registeredName = triggerName.substring(prefixIndex);
        Configuration configuration = CONFIGURATIONS.get(registeredName);
        if (configuration == null) {
            throw new IllegalStateException("No H2 many-to-many trigger configuration registered for " + registeredName);
        }
        apply(configuration);
    }

    public static String configurationId(String signature) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(signature.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    public static void registerConfiguration(
            String triggerName,
            String holderSchema,
            String holderTable,
            String joinSchema,
            String joinTable,
            String targetSchema,
            String targetTable,
            DeleteStrategy deleteStrategy,
            List<Link> joinTableToHolderLinks,
            List<Link> joinTableToTargetLinks
    ) {
        Configuration configuration = new Configuration(
                holderSchema,
                holderTable,
                joinSchema,
                joinTable,
                targetSchema,
                targetTable,
                deleteStrategy,
                List.copyOf(joinTableToHolderLinks),
                List.copyOf(joinTableToTargetLinks)
        );
        Configuration existing = CONFIGURATIONS.putIfAbsent(triggerName, configuration);
        if (existing != null && !existing.equals(configuration)) {
            throw new IllegalStateException("Conflicting H2 many-to-many trigger configuration for " + triggerName);
        }
    }

    private void apply(Configuration configuration) {
        holderSchema = configuration.holderSchema();
        holderTable = configuration.holderTable();
        joinSchema = configuration.joinSchema();
        joinTable = configuration.joinTable();
        targetSchema = configuration.targetSchema();
        targetTable = configuration.targetTable();
        deleteStrategy = configuration.deleteStrategy();
        joinTableToHolderLinks = configuration.joinTableToHolderLinks();
        joinTableToTargetLinks = configuration.joinTableToTargetLinks();
    }

    private record Configuration(
            String holderSchema,
            String holderTable,
            String joinSchema,
            String joinTable,
            String targetSchema,
            String targetTable,
            DeleteStrategy deleteStrategy,
            List<Link> joinTableToHolderLinks,
            List<Link> joinTableToTargetLinks
    ) {
    }

    @Override
    public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
        if (newRow != null || oldRow == null) {
            return;
        }
        loadHolderColumnNames(connection, oldRow.length);
        List<Object> holderValues = getHolderValues(oldRow);

        if (deleteStrategy == DeleteStrategy.CASCADE) {
            deleteTargets(connection, holderValues);
        }
        deleteJoinEntries(connection, holderValues);
    }

    private List<Object> getHolderValues(Object[] oldRow) throws SQLException {
        List<Object> holderValues = new ArrayList<>();
        for (Link link : joinTableToHolderLinks) {
            int holderColumnIndex = holderColumnNames.indexOf(link.columnInReferencedTable());
            if (holderColumnIndex < 0) {
                throw new SQLException("Could not find holder column " + link.columnInReferencedTable());
            }
            holderValues.add(oldRow[holderColumnIndex]);
        }
        return holderValues;
    }

    private void deleteTargets(Connection connection, List<Object> holderValues) throws SQLException {
        StringBuilder select = new StringBuilder("SELECT ");
        for (Link link : joinTableToTargetLinks) {
            select.append("\"").append(link.columnInReferringTable()).append("\", ");
        }
        select.setLength(select.length() - 2);
        select.append(" FROM \"").append(joinSchema).append("\".\"").append(joinTable).append("\" WHERE ");
        appendJoinHolderPredicate(select);

        List<Object[]> targetIds = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(select.toString())) {
            setParameters(statement, holderValues);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Object[] targetId = new Object[joinTableToTargetLinks.size()];
                    for (int i = 0; i < targetId.length; i++) {
                        targetId[i] = resultSet.getObject(i + 1);
                    }
                    targetIds.add(targetId);
                }
            }
        }

        StringBuilder delete = new StringBuilder("DELETE FROM \"").append(targetSchema).append("\".\"")
                .append(targetTable).append("\" WHERE ");
        for (Link link : joinTableToTargetLinks) {
            delete.append("\"").append(link.columnInReferencedTable()).append("\" = ? AND ");
        }
        delete.setLength(delete.length() - 5);
        logger.debug("Executing many-to-many cascade delete: {}", delete);

        try (PreparedStatement statement = connection.prepareStatement(delete.toString())) {
            for (Object[] targetId : targetIds) {
                for (int i = 0; i < targetId.length; i++) {
                    statement.setObject(i + 1, targetId[i]);
                }
                statement.addBatch();
            }
            if (!targetIds.isEmpty()) {
                statement.executeBatch();
            }
        }
    }

    private void deleteJoinEntries(Connection connection, List<Object> holderValues) throws SQLException {
        StringBuilder delete = new StringBuilder("DELETE FROM \"").append(joinSchema).append("\".\"")
                .append(joinTable).append("\" WHERE ");
        appendJoinHolderPredicate(delete);
        logger.debug("Executing many-to-many join cleanup: {}", delete);

        try (PreparedStatement statement = connection.prepareStatement(delete.toString())) {
            setParameters(statement, holderValues);
            statement.executeUpdate();
        }
    }

    private void appendJoinHolderPredicate(StringBuilder sql) {
        for (Link link : joinTableToHolderLinks) {
            sql.append("\"").append(link.columnInReferringTable()).append("\" = ? AND ");
        }
        sql.setLength(sql.length() - 5);
    }

    private void setParameters(PreparedStatement statement, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            statement.setObject(i + 1, values.get(i));
        }
    }

    private void loadHolderColumnNames(Connection connection, int expectedColumnCount) throws SQLException {
        if (holderColumnNames.size() == expectedColumnCount) {
            return;
        }
        List<String> columns = new ArrayList<>(expectedColumnCount);
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
        )) {
            statement.setString(1, holderSchema);
            statement.setString(2, holderTable);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        }
        holderColumnNames.clear();
        holderColumnNames.addAll(columns);
    }

}
