package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.account.AccountStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

public class EnumTest extends DataTest {

    @Test
    public void testEnums() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(AccountStatus.class);
        dataManager.finishLoading();

        AccountStatus status = AccountStatus.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        assertSame(AccountStatus.Status.ACTIVE, status.getStatus());

        status.setStatus(AccountStatus.Status.INACTIVE);

        assertSame(AccountStatus.Status.INACTIVE, status.getStatus());

    }
}
