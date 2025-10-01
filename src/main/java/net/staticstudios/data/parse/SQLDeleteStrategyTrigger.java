package net.staticstudios.data.parse;

import net.staticstudios.data.DeleteStrategy;
import net.staticstudios.data.impl.h2.trigger.H2DeleteStrategyCascadeTrigger;
import org.intellij.lang.annotations.Language;

import java.util.Set;

public class SQLDeleteStrategyTrigger implements SQLTrigger {
    private final String parentSchema;
    private final String parentTable;
    private final String targetSchema;
    private final String targetTable;
    private final DeleteStrategy deleteStrategy;
    private final Set<ForeignKey.Link> links;

    public SQLDeleteStrategyTrigger(String parentSchema, String parentTable, String targetSchema, String targetTable, DeleteStrategy deleteStrategy, Set<ForeignKey.Link> links) {
        this.parentSchema = parentSchema;
        this.parentTable = parentTable;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.deleteStrategy = deleteStrategy;
        this.links = links;
    }

    @Override
    public String getPgSQL() {
        if (deleteStrategy == DeleteStrategy.CASCADE) {
            @Language("SQL") String createTriggerFunction = """
                    CREATE OR REPLACE FUNCTION static_data_v3_%s_%s_%s_%s_delete_trigger()
                    RETURNS TRIGGER AS $$
                     BEGIN
                        %s
                        RETURN OLD;
                    END;
                    $$ LANGUAGE plpgsql;
                    
                    DROP TRIGGER IF EXISTS static_data_v3_%s_%s_%s_%s_delete_trigger ON %s.%s;
                    CREATE TRIGGER static_data_v3_%s_%s_%s_%s_delete_trigger
                    AFTER DELETE ON %s.%s
                    FOR EACH ROW EXECUTE FUNCTION static_data_v3_%s_%s_%s_%s_delete_trigger();
                    """;


            String action = "DELETE FROM \"" + targetSchema + "\".\"" + targetTable + "\" WHERE " +
                    String.join(" AND ", links.stream().map(link -> String.format("%s = OLD.%s", link.columnInReferencedTable(), link.columnInReferringTable())).toList()) + ";";

            return String.format(createTriggerFunction,
                    parentSchema, parentTable, targetSchema, targetTable,
                    action,
                    parentSchema, parentTable, targetSchema, targetTable,
                    parentSchema, parentTable,
                    parentSchema, parentTable, targetSchema, targetTable,
                    parentSchema, parentTable,
                    parentSchema, parentTable, targetSchema,
                    targetTable
            );
        }
        return "DROP TRIGGER IF EXISTS static_data_v3_" + parentSchema + "_" + parentTable + "_" + targetSchema + "_" + targetTable + "_delete_trigger ON " + parentSchema + "." + parentTable + ";";
    }

    @Override
    public String getH2SQL() {
        if (deleteStrategy == DeleteStrategy.CASCADE) {
            String triggerClass = H2DeleteStrategyCascadeTrigger.class.getName();
            String encodedLinks = String.join("", links.stream().map(link -> prefixString(link.columnInReferringTable()) + prefixString(link.columnInReferencedTable())).toList());

            String triggerName = "static_data_v3_" + prefixString(parentSchema) + "_" + prefixString(parentTable) + "_" + prefixString(targetSchema) + "_" + prefixString(targetTable) + "__delete_links__" + (links.size() * 2) + "_" + encodedLinks;
            return "CREATE TRIGGER IF NOT EXISTS \"" + triggerName + "_delete_trigger\" AFTER DELETE ON \"" + parentSchema + "\".\"" + parentTable + "\" FOR EACH ROW CALL \"" + triggerClass + "\"";
        }
        return "";
    }

    private String prefixString(String str) {
        return str.length() + "_" + str;
    }

}
