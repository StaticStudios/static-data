package net.staticstudios.data;

public class StaticDataStatistics {
    private long queriesPerSecond = -1;
    private long updatesPerSecond = -1;
    private int relationCacheSize = -1;
    private int dependenciesToRelationsCacheMappingSize = -1;
    private int cellCacheSize = -1;
    private int dependenciesToCellCacheMappingSize = -1;

    public void setQueriesPerSecond(long queriesPerSecond) {
        this.queriesPerSecond = queriesPerSecond;
    }

    public void setUpdatesPerSecond(long updatesPerSecond) {
        this.updatesPerSecond = updatesPerSecond;
    }

    public void setRelationCacheSize(int relationCacheSideSize) {
        this.relationCacheSize = relationCacheSideSize;
    }

    public void setDependenciesToRelationsCacheMappingSize(int dependenciesToRelationsCacheMappingSize) {
        this.dependenciesToRelationsCacheMappingSize = dependenciesToRelationsCacheMappingSize;
    }

    public void setCellCacheSize(int cellCacheSize) {
        this.cellCacheSize = cellCacheSize;
    }

    public void setDependenciesToCellCacheMappingSize(int dependenciesToCellCacheMappingSize) {
        this.dependenciesToCellCacheMappingSize = dependenciesToCellCacheMappingSize;
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

    public int getRelationCacheSize() {
        return relationCacheSize;
    }

    public int getDependenciesToRelationsCacheMappingSize() {
        return dependenciesToRelationsCacheMappingSize;
    }

    public int getCellCacheSize() {
        return cellCacheSize;
    }

    public int getDependenciesToCellCacheMappingSize() {
        return dependenciesToCellCacheMappingSize;
    }

}
