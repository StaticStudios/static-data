package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;

import java.util.Objects;

public class ReferenceUpdateHandlerWrapper<U extends UniqueData, T extends UniqueData> {
    private final ReferenceUpdateHandler<U, T> handler;
    private ReferenceMetadata referenceMetadata;

    public ReferenceUpdateHandlerWrapper(ReferenceUpdateHandler<U, T> handler) {
        LambdaUtils.assertLambdaDoesntCapture(handler, "Use thr provided instance to access member variables.");
        // we don't want to hold a reference to a UniqueData instances, since it won't get GCed
        // and the handler may be called for any holder instance.

        this.handler = handler;
    }

    public void setReferenceMetadata(ReferenceMetadata referenceMetadata) {
        this.referenceMetadata = referenceMetadata;
    }

    public ReferenceMetadata getReferenceMetadata() {
        return referenceMetadata;
    }

    public ReferenceUpdateHandler<U, T> getHandler() {
        return handler;
    }

    public void unsafeHandle(UniqueData holder, Object oldValue, Object newValue) {
        handler.unsafeHandle(holder, oldValue, newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handler, referenceMetadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReferenceUpdateHandlerWrapper<?, ?> that = (ReferenceUpdateHandlerWrapper<?, ?>) obj;
        return referenceMetadata.equals(that.referenceMetadata) && handler.equals(that.handler);
    }
}
