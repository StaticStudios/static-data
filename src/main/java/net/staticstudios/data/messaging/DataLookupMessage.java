package net.staticstudios.data.messaging;

import java.util.List;
import java.util.UUID;

/**
 * A message to locate a piece of data
 *
 * @param topLevelTable The top level table the object is in
 * @param tables        The tables the object is in
 * @param uniqueId      The unique id of the object
 */
public record DataLookupMessage(String topLevelTable, List<String> tables, UUID uniqueId) {
}
