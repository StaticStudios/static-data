package net.staticstudios.data;


import com.google.common.base.Preconditions;
import net.staticstudios.data.data.Data;
import net.staticstudios.data.data.DataHolder;
import net.staticstudios.data.data.collection.SimplePersistentCollection;
import net.staticstudios.data.data.value.Value;
import net.staticstudios.data.key.UniqueIdentifier;
import net.staticstudios.data.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a unique data object that is stored in the database and contains other data objects.
 */
public abstract class UniqueData implements DataHolder {
    private final DataManager dataManager;
    private final String schema;
    private final String table;
    private final UniqueIdentifier identifier;

    /**
     * Create a new unique data object.
     * The id column is assumed to be "id".
     * See {@link #UniqueData(DataManager, String, String, String, UUID)} if the id column is different.
     *
     * @param dataManager the data manager responsible for this data object
     * @param schema      the schema of the table
     * @param table       the table name
     * @param id          the id of the data object
     */
    protected UniqueData(DataManager dataManager, String schema, String table, UUID id) {
        this(dataManager, schema, table, "id", id);
    }

    /**
     * Create a new unique data object.
     *
     * @param dataManager the data manager responsible for this data object
     * @param schema      the schema of the table
     * @param table       the table name
     * @param idColumn    the name of the column that stores the id
     * @param id          the id of the data object
     */
    protected UniqueData(DataManager dataManager, String schema, String table, String idColumn, UUID id) {
        Preconditions.checkArgument(dataManager.get(this.getClass(), id) == null, "Data with id %s already exists", id);
        this.dataManager = dataManager;
        this.schema = schema;
        this.table = table;
        this.identifier = UniqueIdentifier.of(idColumn, id);
    }

    /**
     * Get the id of this data object.
     *
     * @return the id
     */
    public UUID getId() {
        return identifier.getId();
    }

    /**
     * Get the table that this data object is stored in.
     *
     * @return the table name
     */
    public String getTable() {
        return table;
    }

    /**
     * Get the schema that this data object is stored in.
     *
     * @return the schema name
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Get the unique identifier of this data object.
     *
     * @return the unique identifier
     */
    public UniqueIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public DataManager getDataManager() {
        return dataManager;
    }

    @Override
    public UniqueData getRootHolder() {
        return this;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UniqueData that = (UniqueData) obj;
        return Objects.equals(getId(), that.getId());
    }

    @Override
    public final int hashCode() {
        UUID id = getId();
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
        sb.append("{");
        sb.append("id=").append(getId()).append("(").append(getSchema()).append(".").append(getTable()).append(".").append(identifier.getColumn()).append(")");

        for (Field field : ReflectionUtils.getFields(this.getClass())) {
            field.setAccessible(true);
            Class<?> fieldClass = field.getType();

            if (!Data.class.isAssignableFrom(fieldClass)) {
                continue;
            }

            try {
                Data<?> data = (Data<?>) field.get(this);

                if (data instanceof Value<?> value) {
                    sb.append(", ");
                    sb.append(field.getName()).append("=").append(value.get());
                } else if (data instanceof SimplePersistentCollection<?> collection) {
                    sb.append(", ");
                    sb.append(field.getName()).append("=Collection<").append(collection.getDataType().getSimpleName()).append(">{size=").append(collection.size()).append("}");
                } else if (data instanceof Reference<?> reference) {
                    sb.append(", ");
                    UniqueData ref = reference.get();
                    if (ref == null) {
                        sb.append(field.getName()).append("=null");
                    } else {
                        sb.append(field.getName()).append("=").append(ref.getClass().getSimpleName()).append("{id=").append(ref.getId()).append("}");
                    }
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        sb.append("}");

        return sb.toString();
    }
}
