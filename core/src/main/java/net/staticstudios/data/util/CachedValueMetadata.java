package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public final class CachedValueMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final String holderSchema;
    private final String holderTable;
    private final String identifier;
    private final Object fallbackValue;
    private final Class<?> type;
    private final int expireAfterSeconds;
    private final int updateInterval;
    private boolean validatedRefresher = false;
    private boolean validatedUpdateHandlers = false;

    public CachedValueMetadata(Class<? extends UniqueData> holderClass, String holderSchema, String holderTable,
                               String identifier, Object fallbackValue, Class<?> type, int expireAfterSeconds,
                               int updateInterval) {
        this.holderClass = holderClass;
        this.holderSchema = holderSchema;
        this.holderTable = holderTable;
        this.identifier = identifier;
        this.fallbackValue = fallbackValue;
        this.type = type;
        this.expireAfterSeconds = expireAfterSeconds;
        this.updateInterval = updateInterval;
    }

    public Class<? extends UniqueData> holderClass() {
        return holderClass;
    }

    public String holderSchema() {
        return holderSchema;
    }

    public String holderTable() {
        return holderTable;
    }

    public String identifier() {
        return identifier;
    }

    public Object fallbackValue() {
        return fallbackValue;
    }

    public Class<?> type() {
        return type;
    }

    public int expireAfterSeconds() {
        return expireAfterSeconds;
    }

    public int updateInterval() {
        return updateInterval;
    }

    public boolean hasValidatedRefresher() {
        return validatedRefresher;
    }

    public void setValidatedRefresher(boolean validatedRefresher) {
        this.validatedRefresher = validatedRefresher;
    }

    public boolean hasValidatedUpdateHandlers() {
        return validatedUpdateHandlers;
    }

    public void setValidatedUpdateHandlers(boolean validatedUpdateHandlers) {
        this.validatedUpdateHandlers = validatedUpdateHandlers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (CachedValueMetadata) obj;
        return Objects.equals(this.holderClass, that.holderClass) &&
                Objects.equals(this.holderSchema, that.holderSchema) &&
                Objects.equals(this.holderTable, that.holderTable) &&
                Objects.equals(this.identifier, that.identifier) &&
                Objects.equals(this.fallbackValue, that.fallbackValue) &&
                Objects.equals(this.type, that.type) &&
                this.expireAfterSeconds == that.expireAfterSeconds &&
                this.updateInterval == that.updateInterval;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderClass, holderSchema, holderTable, identifier, type, expireAfterSeconds, updateInterval);
    }

    @Override
    public String toString() {
        return "CachedValueMetadata[" +
                "holderClass=" + holderClass + ", " +
                "holderSchema=" + holderSchema + ", " +
                "holderTable=" + holderTable + ", " +
                "identifier=" + identifier + ", " +
                "type=" + type + ", " +
                "expireAfterSeconds=" + expireAfterSeconds + ", " +
                "updateInterval=" + updateInterval + ']';
    }

}
