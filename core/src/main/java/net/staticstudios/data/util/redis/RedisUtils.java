package net.staticstudios.data.util.redis;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.parse.SQLColumn;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.primative.Primitives;
import net.staticstudios.data.util.ColumnMetadata;
import net.staticstudios.data.util.ColumnValuePair;
import net.staticstudios.data.util.ColumnValuePairs;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RedisUtils {
    public static Pattern globToRegex(String redisPattern) {
        StringBuilder regex = new StringBuilder();

        for (int i = 0; i < redisPattern.length(); i++) {
            char c = redisPattern.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                case '[', ']' -> regex.append(c);
                case '\\' -> regex.append("\\\\");
                default -> {
                    if ("+()^$.{}|".indexOf(c) != -1) {
                        regex.append('\\');
                    }
                    regex.append(c);
                }
            }
        }

        return Pattern.compile(regex.toString());
    }

    public static @Nullable RedisIdentifier fromKey(String key, DataManager dataManager) {
        String[] parts = key.split(":");
        String holderSchema = parts[1];
        String holderTable = parts[2];
        String identifier = parts[parts.length - 1];

        SQLSchema schema = dataManager.getSQLBuilder().getSchema(holderSchema);
        if (schema == null) {
            return null;
        }

        SQLTable table = schema.getTable(holderTable);
        if (table == null) {
            return null;
        }

        List<ColumnValuePair> idColumns = new ArrayList<>();
        for (int i = 3; i < parts.length - 1; i += 2) {
            String value = parts[i + 1];
            SQLColumn column = table.getColumn(parts[i]);
            if (column == null) {
                return null;
            }

            idColumns.add(new ColumnValuePair(parts[i], Primitives.decodePrimitive(column.getType(), value)));
        }
        return new RedisIdentifier(holderSchema, holderTable, identifier, new ColumnValuePairs(idColumns.toArray(ColumnValuePair[]::new)));
    }

    public static String toKey(RedisIdentifier identifier) {
        return buildRedisKey(identifier.holderSchema(), identifier.holderTable(), identifier.identifier(), identifier.idColumns());
    }

    public static String buildRedisKey(String holderSchema, String holderTable, String identifier, ColumnValuePairs idColumns) {
        //static-data:[schema]:[table]:[id column-value pairs, seperated by ':']:[identifier]
        StringBuilder sb = new StringBuilder("static-data:");
        sb.append(holderSchema).append(":").append(holderTable).append(":");
        for (ColumnValuePair pair : idColumns) {
            sb.append(pair.column()).append(":").append(pair.value()).append(":");
        }
        sb.append(identifier);
        return sb.toString();
    }

    public static String buildPartialRedisKey(String holderSchema, String holderTable, String identifier, List<ColumnMetadata> idColumnMetadata) {
        StringBuilder sb = new StringBuilder("static-data:");
        sb.append(holderSchema).append(":").append(holderTable).append(":");
        for (ColumnMetadata idColumn : idColumnMetadata) {
            sb.append(idColumn.name()).append(":").append("*").append(":");
        }
        sb.append(identifier);
        return sb.toString();
    }

    public static DeconstructedKey deconstruct(String key) {
        List<String> encodedIdValues = new ArrayList<>();
        List<String> encodedIdNames = new ArrayList<>();
        String[] parts = key.split(":");
        StringBuilder sb = new StringBuilder();
        sb.append(parts[0]).append(":").append(parts[1]).append(":").append(parts[2]).append(":");
        for (int i = 3; i < parts.length - 1; i += 2) {
            sb.append(parts[i]).append(":").append("*").append(":");
            encodedIdNames.add(parts[i]);
            encodedIdValues.add(parts[i + 1]);
        }
        sb.append(parts[parts.length - 1]);
        return new DeconstructedKey(sb.toString(), encodedIdNames, encodedIdValues);
    }

    public static FullyDeconstructedKey fullyDeconstruct(String key, DataManager dataManager) {
        String[] parts = key.split(":");
        String holderSchema = parts[1];
        String holderTable = parts[2];
        String identifier = parts[parts.length - 1];

        SQLSchema schema = dataManager.getSQLBuilder().getSchema(holderSchema);
        if (schema == null) {
            return null;
        }

        SQLTable table = schema.getTable(holderTable);
        if (table == null) {
            return null;
        }

        List<ColumnValuePair> idColumns = new ArrayList<>();
        for (int i = 3; i < parts.length - 1; i += 2) {

            for (ColumnMetadata columnMetadata : table.getIdColumns()) {
                if (columnMetadata.name().equals(parts[i])) {
                    idColumns.add(new ColumnValuePair(parts[i], Primitives.decodePrimitive(columnMetadata.type(), parts[i + 1])));
                    break;
                }
            }
        }
        return new FullyDeconstructedKey(holderSchema, holderTable, identifier, new ColumnValuePairs(idColumns.toArray(ColumnValuePair[]::new)));
    }

    public record DeconstructedKey(String partialKey, List<String> encodedIdNames, List<String> encodedIdValues) {

    }

    public record FullyDeconstructedKey(String holderSchema, String holderTable, String identifier,
                                        ColumnValuePairs idColumns) {

    }


    public static String getVirtualColumnName(String identifier) {
        return "__virtual__cv_" + identifier;
    }
}
