package net.staticstudios.data.impl.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import net.staticstudios.data.CachedValue;
import net.staticstudios.data.ExpireAfter;
import net.staticstudios.data.Identifier;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.util.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CachedValueImpl<T> extends AbstractCachedValue<T> {
    private final UniqueData holder;
    private final Class<T> dataType;
    private final CachedValueMetadata metadata;

    private CachedValueImpl(UniqueData holder, Class<T> dataType, CachedValueMetadata metadata) {
        this.holder = holder;
        this.dataType = dataType;
        this.metadata = metadata;
    }

    public static <T> void createAndDelegate(ProxyCachedValue<T> proxy, CachedValueMetadata metadata) {
        CachedValueImpl<T> delegate = new CachedValueImpl<>(
                proxy.getHolder(),
                proxy.getDataType(),
                metadata
        );

        proxy.setDelegate(metadata, delegate);
    }

    public static <T> CachedValueImpl<T> create(UniqueData holder, Class<T> dataType, CachedValueMetadata metadata) {
        return new CachedValueImpl<>(holder, dataType, metadata);
    }

    public static <T extends UniqueData> void delegate(T instance) {
        UniqueDataMetadata metadata = instance.getDataManager().getMetadata(instance.getClass());
        for (FieldInstancePair<@Nullable CachedValue> pair : ReflectionUtils.getFieldInstancePairs(instance, CachedValue.class)) {
            CachedValueMetadata pvMetadata = metadata.cachedValueMetadata().get(pair.field());
            if (pair.instance() instanceof CachedValue.ProxyCachedValue<?> proxyCv) {
                CachedValueImpl.createAndDelegate(proxyCv, pvMetadata);
            } else {
                pair.field().setAccessible(true);
                try {
                    pair.field().set(instance, CachedValueImpl.create(instance, ReflectionUtils.getGenericType(pair.field()), pvMetadata));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static <T extends UniqueData> Map<Field, CachedValueMetadata> extractMetadata(String holderSchema, String holderTable, Class<T> clazz) {
        Map<Field, CachedValueMetadata> metadataMap = new HashMap<>();
        for (Field field : ReflectionUtils.getFields(clazz, CachedValue.class)) {
            metadataMap.put(field, extractMetadata(holderSchema, holderTable, clazz, field));
        }
        return metadataMap;
    }

    public static <T extends UniqueData> CachedValueMetadata extractMetadata(String holderSchema, String holderTable, Class<T> clazz, Field field) {
        Identifier identifierAnnotation = field.getAnnotation(Identifier.class);
        ExpireAfter expireAfterAnnotation = field.getAnnotation(ExpireAfter.class);


        Preconditions.checkNotNull(identifierAnnotation, "CachedValue field %s is missing @Identifier annotation".formatted(field.getName()));

        int expireAfterSeconds = -1;
        if (expireAfterAnnotation != null) {
            expireAfterSeconds = expireAfterAnnotation.value();
        }

        return new CachedValueMetadata(clazz, holderSchema, holderTable, ValueUtils.parseValue(identifierAnnotation.value()), expireAfterSeconds);
    }


    @Override
    public UniqueData getHolder() {
        return holder;
    }

    @Override
    public Class<T> getDataType() {
        return dataType;
    }

    @Override
    public <U extends UniqueData> CachedValue<T> onUpdate(Class<U> holderClass, ValueUpdateHandler<U, T> updateHandler) {
        throw new UnsupportedOperationException("Dynamically adding update handlers is not supported");
    }

    @Override
    public CachedValue<T> supplyFallback(Supplier<T> fallback) {
        throw new UnsupportedOperationException("Cannot set fallback after initialization");
    }

    @Override
    public CachedValue<T> refresh(Supplier<T> refresh) {
        throw new UnsupportedOperationException("Cannot set refresh after initialization");
    }

    @Override
    public T get() {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot get value from a deleted UniqueData instance");
        T value = holder.getDataManager().getRedis(metadata.holderSchema(), metadata.holderTable(), metadata.identifier(), holder.getIdColumns(), dataType);
        if (value == null) {
            value = refresh();
            value = value == null ? getFallback() : value;
        }
        return value;
    }

    @Override
    public void set(@Nullable T value) {
        Preconditions.checkArgument(!holder.isDeleted(), "Cannot set value on a deleted UniqueData instance");
        T fallback = getFallback();
        T toSet;
        if (Objects.equals(fallback, value)) {
            toSet = null;
        } else {
            toSet = value;
        }
        holder.getDataManager().setRedis(metadata.holderSchema(), metadata.holderTable(), metadata.identifier(), holder.getIdColumns(), metadata.expireAfterSeconds(), toSet);
    }

    @Override
    public String toString() {
        if (holder.isDeleted()) {
            return "[DELETED]";
        }
        return String.valueOf(get());
    }
}
