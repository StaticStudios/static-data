package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public final class CachedValueMetadata {
    private final Class<? extends UniqueData> holderClass;
    private final String holderSchema;
    private final String holderTable;
    private final String identifier;
    private final Class<?> type;
    private final int expireAfterSeconds;
    private boolean validatedFallbackSupplier = false;
    private boolean validatedRefresher = false;
    private boolean validatedUpdateHandlers = false;

    public CachedValueMetadata(Class<? extends UniqueData> holderClass, String holderSchema, String holderTable,
                               String identifier, Class<?> type, int expireAfterSeconds) {
        this.holderClass = holderClass;
        this.holderSchema = holderSchema;
        this.holderTable = holderTable;
        this.identifier = identifier;
        this.type = type;
        this.expireAfterSeconds = expireAfterSeconds;
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

    public Class<?> type() {
        return type;
    }

    public int expireAfterSeconds() {
        return expireAfterSeconds;
    }

    public boolean hasValidatedFallbackSupplier() {
        return validatedFallbackSupplier;
    }

    public void setValidatedFallbackSupplier(boolean validatedFallbackSupplier) {
        this.validatedFallbackSupplier = validatedFallbackSupplier;
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
                Objects.equals(this.type, that.type) &&
                this.expireAfterSeconds == that.expireAfterSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(holderClass, holderSchema, holderTable, identifier, type, expireAfterSeconds);
    }

    @Override
    public String toString() {
        return "CachedValueMetadata[" +
                "holderClass=" + holderClass + ", " +
                "holderSchema=" + holderSchema + ", " +
                "holderTable=" + holderTable + ", " +
                "identifier=" + identifier + ", " +
                "type=" + type + ", " +
                "expireAfterSeconds=" + expireAfterSeconds + ']';
    }

}
