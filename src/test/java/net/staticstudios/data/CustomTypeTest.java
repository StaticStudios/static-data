package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.account.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class CustomTypeTest extends DataTest {
    @Test
    public void testCustomTypesSetGet() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(new AccountDetailsValueSerializer());
        dataManager.registerValueSerializer(new AccountSettingsValueSerializer());
        dataManager.load(MockAccount.class);

        MockAccount account = MockAccountFactory.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        assertNull(account.settings.get());
        assertNull(account.details.get());

        AccountDetails details = new AccountDetails("detail1", "detail2");
        AccountSettings settings = new AccountSettings(true, false);
        account.settings.set(settings);
        account.details.set(details);

        assertEquals(settings, account.settings.get());
        assertEquals(details, account.details.get());
    }

    @Test
    public void testCustomTypesLoad() throws SQLException {
        AccountDetailsValueSerializer detailsSerializer = new AccountDetailsValueSerializer();
        AccountSettingsValueSerializer settingsSerializer = new AccountSettingsValueSerializer();
        AccountDetails details = new AccountDetails("detail1", "detail2");
        AccountSettings settings = new AccountSettings(true, false);

        String detailsSerialized = detailsSerializer.serialize(details);
        String settingsSerialized = settingsSerializer.serialize(settings);

        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE \"public\".\"accounts\" (\"id\" INTEGER PRIMARY KEY, \"settings\" TEXT NULL);");
            statement.execute("CREATE TABLE \"public\".\"account_details\" (\"account_id\" INTEGER PRIMARY KEY, \"details\" TEXT NULL, FOREIGN KEY (\"account_id\") REFERENCES \"accounts\"(\"id\") ON DELETE CASCADE);");

            statement.execute("INSERT INTO \"public\".\"accounts\" (\"id\", \"settings\") VALUES (1, '" + settingsSerialized + "');");
            statement.execute("INSERT INTO \"public\".\"account_details\" (\"account_id\", \"details\") VALUES (1, '" + detailsSerialized + "');");
        }

        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.registerValueSerializer(detailsSerializer);
        dataManager.registerValueSerializer(settingsSerializer);
        dataManager.load(MockAccount.class);

        MockAccount account = MockAccountQuery.where(dataManager).idIs(1).findOne();
        assertNotNull(account);
        assertEquals(settings, account.settings.get());
        assertEquals(details, account.details.get());
    }

    //todo: test postgres listen/notify with custom types
}