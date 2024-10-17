package net.staticstudios.data.mocks;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.shared.CollectionEntry;
import net.staticstudios.data.value.PersistentEntryValue;

import java.util.UUID;

public class MockLocation extends CollectionEntry {
    private final PersistentEntryValue<UUID> id = PersistentEntryValue.immutable(this, UUID.class, "id");
    private final PersistentEntryValue<Integer> x = PersistentEntryValue.mutable(this, Integer.class, "x");
    private final PersistentEntryValue<Integer> y = PersistentEntryValue.mutable(this, Integer.class, "y");
    private final PersistentEntryValue<Integer> z = PersistentEntryValue.mutable(this, Integer.class, "z");


    @SuppressWarnings("unused")
    private MockLocation() {
        super();
    }

    public MockLocation(DataManager dataManager, int x, int y, int z) {
        super(dataManager);
        this.id.setInitialValue(UUID.randomUUID());
        this.x.setInitialValue(x);
        this.y.setInitialValue(y);
        this.z.setInitialValue(z);
    }

    public int getX() {
        return x.get();
    }

    public int getY() {
        return y.get();
    }

    public int getZ() {
        return z.get();
    }

    public void setX(int x) {
        this.x.set(x);
    }

    public void setY(int y) {
        this.y.set(y);
    }

    public void setZ(int z) {
        this.z.set(z);
    }
}
