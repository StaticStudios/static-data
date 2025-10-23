package net.staticstudios.data.mock.user;

import net.staticstudios.data.*;

import java.sql.Timestamp;
import java.util.UUID;

@Data(schema = "public", table = "user_sessions")
public class MockUserSession extends UniqueData {
    @IdColumn(name = "session_id")
    public PersistentValue<UUID> id;

    @Column(name = "user_id", nullable = true)
    public PersistentValue<UUID> userId;

    @OneToOne(link = "user_id=id")
    public Reference<MockUser> user;

    @Column(name = "timestamp")
    public PersistentValue<Timestamp> timestamp;
}
