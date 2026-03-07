package net.staticstudios.data;

public class StaticDataStatistics {
    private final long queriesPerSecond;
    private final long updatesPerSecond;

    public StaticDataStatistics(long queriesPerSecond, long updatesPerSecond) {
        this.queriesPerSecond = queriesPerSecond;
        this.updatesPerSecond = updatesPerSecond;
    }

    public long getQueriesPerSecond() {
        return queriesPerSecond;
    }

    public long getUpdatesPerSecond() {
        return updatesPerSecond;
    }

    public long getOperationsPerSecond() {
        return queriesPerSecond + updatesPerSecond;
    }
}
