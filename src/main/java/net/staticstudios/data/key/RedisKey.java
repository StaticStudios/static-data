package net.staticstudios.data.key;

import com.google.common.base.Preconditions;
import net.staticstudios.data.CachedValue;

import java.util.UUID;

public class RedisKey extends DataKey {
    private final String identifyingKey;
    private final String holderSchema;
    private final String holderTable;
    private final String holderIdColumn;
    private final UUID rootHolderId;

    public RedisKey(String holderSchema, String holderTable, String holderIdColumn, UUID rootHolderId, String identifyingKey) {
        super(holderSchema, holderTable, holderIdColumn, rootHolderId, identifyingKey);
        Preconditions.checkArgument(!identifyingKey.contains(":"), "Identifying key cannot contain ':'");
        this.identifyingKey = identifyingKey;
        this.holderSchema = holderSchema;
        this.holderTable = holderTable;
        this.holderIdColumn = holderIdColumn;
        this.rootHolderId = rootHolderId;
    }

    public RedisKey(CachedValue<?> data) {
        this(
                data.getSchema(),
                data.getTable(),
                data.getIdColumn(),
                data.getHolder().getRootHolder().getRootHolder().getId(),
                data.getIdentifyingKey()
        );
    }

    public static RedisKey fromString(String key) {
        if (!key.startsWith("static-data:")) {
            return null;
        }
        String[] parts = key.split(":");
        return new RedisKey(parts[1], parts[2], parts[3], UUID.fromString(parts[4]), parts[5]);
    }

    public static boolean isRedisKey(String key) {
        return fromString(key) != null;
    }

    /**
     * Omit the root holder id from the key
     *
     * @return the partial key
     */
    public String toPartialKey() {
        return String.format("static-data:%s:%s:%s:*:%s", holderSchema, holderTable, holderIdColumn, identifyingKey);
    }

    public String getIdentifyingKey() {
        return identifyingKey;
    }

    public String getHolderSchema() {
        return holderSchema;
    }

    public String getHolderTable() {
        return holderTable;
    }

    public String getHolderIdColumn() {
        return holderIdColumn;
    }

    public UUID getRootHolderId() {
        return rootHolderId;
    }

    @Override
    public String toString() {
        return String.format("static-data:%s:%s:%s:%s:%s", holderSchema, holderTable, holderIdColumn, rootHolderId, identifyingKey);
    }
}
