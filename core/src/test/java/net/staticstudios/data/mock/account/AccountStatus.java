package net.staticstudios.data.mock.account;

import net.staticstudios.data.*;

@Data( schema = "public", table = "test_account_status")
public class AccountStatus extends UniqueData {

    @IdColumn(name = "id")
    public PersistentValue<Integer> id;
    @Column(name = "status")

    @DefaultValue("ACTIVE")
    public PersistentValue<Status> status;

    public Status getStatus() {
        return status.get();
    }

    public void setStatus(Status status) {
        this.status.set(status);
    }

    public enum Status {
        ACTIVE, INACTIVE
    }
}
