package net.staticstudios.data.parse;

import net.staticstudios.data.DeleteStrategy;
import net.staticstudios.data.impl.h2.trigger.H2ManyToManyDeleteStrategyTrigger;
import net.staticstudios.data.utils.Link;
import org.intellij.lang.annotations.Language;

import java.util.List;

public class SQLManyToManyDeleteStrategyTrigger implements SQLTrigger {
    private final String holderSchema;
    private final String holderTable;
    private final String joinSchema;
    private final String joinTable;
    private final String targetSchema;
    private final String targetTable;
    private final DeleteStrategy deleteStrategy;
    private final List<Link> joinTableToHolderLinks;
    private final List<Link> joinTableToTargetLinks;

    public SQLManyToManyDeleteStrategyTrigger(
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
        this.holderSchema = holderSchema;
        this.holderTable = holderTable;
        this.joinSchema = joinSchema;
        this.joinTable = joinTable;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.deleteStrategy = deleteStrategy;
        this.joinTableToHolderLinks = List.copyOf(joinTableToHolderLinks);
        this.joinTableToTargetLinks = List.copyOf(joinTableToTargetLinks);
    }

    @Override
    public String getPgSQL() {
        String triggerName = postgresTriggerName();
        if (deleteStrategy == DeleteStrategy.NO_ACTION) {
            return "DROP TRIGGER IF EXISTS \"" + triggerName + "\" ON \"" + holderSchema + "\".\"" + holderTable + "\";";
        }

        @Language("SQL") String createTriggerFunction = """
                CREATE OR REPLACE FUNCTION "%s"()
                RETURNS TRIGGER AS $$
                 BEGIN
                    %s
                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS "%s" ON "%s"."%s";
                CREATE TRIGGER "%s"
                BEFORE DELETE ON "%s"."%s"
                FOR EACH ROW EXECUTE FUNCTION "%s"();
                """;

        StringBuilder action = new StringBuilder();
        if (deleteStrategy == DeleteStrategy.CASCADE) {
            action.append("DELETE FROM \"")
                    .append(targetSchema).append("\".\"").append(targetTable).append("\" AS _target USING \"")
                    .append(joinSchema).append("\".\"").append(joinTable).append("\" AS _join WHERE ");
            appendHolderMatch(action);
            for (Link link : joinTableToTargetLinks) {
                action.append("_target.\"").append(link.columnInReferencedTable()).append("\" = _join.\"")
                        .append(link.columnInReferringTable()).append("\" AND ");
            }
            action.setLength(action.length() - 5);
            action.append(';');
        }
        action.append(" DELETE FROM \"").append(joinSchema).append("\".\"").append(joinTable).append("\" AS _join WHERE ");
        appendHolderMatch(action);
        action.setLength(action.length() - 5);
        action.append(';');

        return createTriggerFunction.formatted(
                triggerName,
                action,
                triggerName, holderSchema, holderTable,
                triggerName, holderSchema, holderTable,
                triggerName
        );
    }

    private void appendHolderMatch(StringBuilder action) {
        for (Link link : joinTableToHolderLinks) {
            action.append("_join.\"").append(link.columnInReferringTable()).append("\" = OLD.\"")
                    .append(link.columnInReferencedTable()).append("\" AND ");
        }
    }

    @Override
    public String getH2SQL() {
        String cascadeTriggerName = compactH2TriggerName(DeleteStrategy.CASCADE);
        String setNullTriggerName = compactH2TriggerName(DeleteStrategy.SET_NULL);
        if (deleteStrategy == DeleteStrategy.NO_ACTION) {
            return "DROP TRIGGER IF EXISTS \"" + holderSchema + "\".\"" + cascadeTriggerName + "\";"
                    + " DROP TRIGGER IF EXISTS \"" + holderSchema + "\".\"" + setNullTriggerName + "\";";
        }
        String triggerName = compactH2TriggerName(deleteStrategy);
        String obsoleteTriggerName = compactH2TriggerName(deleteStrategy == DeleteStrategy.CASCADE ? DeleteStrategy.SET_NULL : DeleteStrategy.CASCADE);
        return "DROP TRIGGER IF EXISTS \"" + holderSchema + "\".\"" + obsoleteTriggerName + "\";"
                + " CREATE TRIGGER IF NOT EXISTS \"" + holderSchema + "\".\"" + triggerName
                + "\" BEFORE DELETE ON \"" + holderSchema + "\".\"" + holderTable + "\" FOR EACH ROW CALL \""
                + H2ManyToManyDeleteStrategyTrigger.class.getName() + "\"";
    }

private String postgresTriggerName() {
    String signature = holderSchema + "." + holderTable + "|" + joinSchema + "." + joinTable + "|" + targetSchema + "." + targetTable;
    return "static_data_v3_m2m_" + Integer.toUnsignedString(signature.hashCode(), 16) + "_delete_trigger";
}

    private String compactH2TriggerName(DeleteStrategy strategy) {
        String signature = h2ConfigurationSignature(strategy);
        String triggerName = "static_data_v3_m2m_" + H2ManyToManyDeleteStrategyTrigger.configurationId(signature);
        H2ManyToManyDeleteStrategyTrigger.registerConfiguration(
                triggerName,
                holderSchema,
                holderTable,
                joinSchema,
                joinTable,
                targetSchema,
                targetTable,
                strategy,
                joinTableToHolderLinks,
                joinTableToTargetLinks
        );
        return triggerName;
    }

    private String h2ConfigurationSignature(DeleteStrategy strategy) {
        return String.join("\0", List.of(
                holderSchema,
                holderTable,
                joinSchema,
                joinTable,
                targetSchema,
                targetTable,
                strategy.name(),
                joinTableToHolderLinks.toString(),
                joinTableToTargetLinks.toString()
        ));
    }
}
