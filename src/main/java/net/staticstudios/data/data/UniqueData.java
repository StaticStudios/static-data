package net.staticstudios.data.data;


import net.staticstudios.data.DataManager;
import net.staticstudios.data.data.collection.PersistentCollection;
import net.staticstudios.data.key.UniqueIdentifier;
import net.staticstudios.data.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.UUID;

public class UniqueData implements DataHolder {
    private final DataManager dataManager;
    private final String schema;
    private final String table;
    private final UniqueIdentifier identifier;

    public UniqueData(DataManager dataManager, String schema, String table, UUID id) {
        this.dataManager = dataManager;
        this.schema = schema;
        this.table = table;
        this.identifier = UniqueIdentifier.of("id", id);
    }

    public UUID getId() {
        return identifier.getId();
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        UniqueData that = (UniqueData) obj;
        return identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
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
                } else if (data instanceof PersistentCollection<?> collection) {
                    sb.append(", ");
                    sb.append(field.getName()).append("=Collection<").append(collection.getDataType().getSimpleName()).append(">{size=").append(collection.size()).append("}");
                } else if (data instanceof Reference<?> reference) {
                    sb.append(", ");
                    sb.append(field.getName()).append("=").append(reference.get().getClass().getSimpleName()).append("{id=").append(reference.get().getId()).append("}");
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        sb.append("}");

        return sb.toString();
    }
}
