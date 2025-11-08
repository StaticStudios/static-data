package net.staticstudios.data.util;

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

    public static String buildRedisKey(String holderSchema, String holderTable, String identifier, ColumnValuePairs icColumns) {
        //static-data:[schema]:[table]:[id column-value pairs, seperated by ':']:[identifier]
        StringBuilder sb = new StringBuilder("static-data:");
        sb.append(holderSchema).append(":").append(holderTable).append(":");
        for (ColumnValuePair pair : icColumns) {
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

    public record DeconstructedKey(String partialKey, List<String> encodedIdNames, List<String> encodedIdValues) {

    }
}
