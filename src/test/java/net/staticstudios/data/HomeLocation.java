package net.staticstudios.data;

import net.staticstudios.data.data.PersistentValue;
import net.staticstudios.data.data.UniqueData;
import org.jetbrains.annotations.Blocking;

import java.util.UUID;

public class HomeLocation extends UniqueData {
    private final PersistentValue<Integer> x = PersistentValue.of(this, Integer.class, "x");
    private final PersistentValue<Integer> y = PersistentValue.of(this, Integer.class, "y");
    private final PersistentValue<Integer> z = PersistentValue.of(this, Integer.class, "z");
//    private final PersistentValue<Backpack> b = PersistentValue.of(this, Backpack.class, "z");

    private HomeLocation(DataManager dataManager, UUID id) {
        super(dataManager, "public", "home_locations", id);
    }

    @Blocking
    public static HomeLocation create(DataManager dataManager, int x, int y, int z) {
        return create(dataManager, UUID.randomUUID(), x, y, z);
    }

    @Blocking
    public static HomeLocation create(DataManager dataManager, UUID id, int x, int y, int z) {
        HomeLocation homeLocation = new HomeLocation(dataManager, id);
        dataManager.insertAsync(homeLocation, homeLocation.x.initial(x), homeLocation.y.initial(y), homeLocation.z.initial(z));
        return homeLocation;
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
}
