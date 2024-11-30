package net.staticstudios.data.mocks.game;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.Table;
import net.staticstudios.data.mocks.MockUser;
import net.staticstudios.data.value.PersistentValue;

import java.util.UUID;

@Table("public.players")
public abstract class MockGenericPlayer extends MockUser {
    private final PersistentValue<Long> money = PersistentValue.withDefault(this, Long.class, 0L, "money");

    @SuppressWarnings("unused")
    protected MockGenericPlayer() {
        super();
    }

    public MockGenericPlayer(DataManager dataManager, UUID id, String name) {
        super(dataManager, id, name);
    }

    public void setMoney(long money) {
        this.money.set(money);
    }

    public long getMoney() {
        return money.get();
    }


    @Override
    public String toString() {
        return "MockPlayer{" +
                "id=" + getId() +
                ", money=" + money +
                "} " + super.toString();

    }
}
