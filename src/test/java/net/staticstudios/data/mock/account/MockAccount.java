package net.staticstudios.data.mock.account;

import net.staticstudios.data.*;

@Data(schema = "public", table = "accounts")
public class MockAccount extends UniqueData {
    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "settings", nullable = true)
    public PersistentValue<AccountSettings> settings;
    @ForeignColumn(name = "details", table = "account_details", nullable = true, link = "id=account_id")
    public PersistentValue<AccountDetails> details;
}
