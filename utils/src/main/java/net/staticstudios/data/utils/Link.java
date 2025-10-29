package net.staticstudios.data.utils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public record Link(String columnInReferencedTable, String columnInReferringTable) {

    public static List<Link> parseRawLinks(String links) {
        List<Link> mappings = new ArrayList<>();
        for (String link : StringUtils.parseCommaSeperatedList(links)) {
            String[] parts = link.split("=");
            Preconditions.checkArgument(parts.length == 2, "Invalid link format! Expected format: localColumn=foreignColumn, got: " + link);
            mappings.add(new Link(parts[1].trim(), parts[0].trim()));
        }

        return mappings;
    }

    public static List<Link> parseRawLinksReversed(String links) {
        List<Link> mappings = new ArrayList<>();
        for (String link : StringUtils.parseCommaSeperatedList(links)) {
            String[] parts = link.split("=");
            Preconditions.checkArgument(parts.length == 2, "Invalid link format! Expected format: localColumn=foreignColumn, got: " + link);
            mappings.add(new Link(parts[0].trim(), parts[1].trim()));
        }

        return mappings;
    }
}
