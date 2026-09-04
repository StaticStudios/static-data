package net.staticstudios.data.benchmark;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import net.staticstudios.data.StaticDataStatistics;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Adds peer writers and PostgreSQL notification listeners to the shared player
 * workload used by the distributed-load benchmarks.
 */
@State(Scope.Group)
public class CrossContainerLoadState extends PlayerWorkloadState {
    private static final long PROPAGATION_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    /** Target rate for each remote writer; zero runs writers at saturation. */
    @Param({"50"})
    public int remoteWritesPerSecond;

    /** Rows partitioned between the peer writers. */
    @Param({"100"})
    public int remoteWritePlayerCount;

    /** PostgreSQL listeners, including the full Static Data listener. */
    @Param({"8"})
    public int listenerCount;

    /** Aggregate compound reads/s in controlled-load groups; zero means saturation. */
    @Param({"250000"})
    public int targetReadsPerSecond;

    private final AtomicInteger nextWriterIndex = new AtomicInteger();
    private final AtomicLong nextReadPermitNanos = new AtomicLong();
    private final List<RemoteWriterSession> writerSessions = new CopyOnWriteArrayList<>();
    private final List<PGConnection> peerListeners = new CopyOnWriteArrayList<>();
    private final LongAdder peerNotifications = new LongAdder();
    private final LongAdder completedControlledReads = new LongAdder();
    private final LongAdder completedPacedRemoteWrites = new LongAdder();
    private final ThreadLocal<RemoteWriterSession> writerSession = new ThreadLocal<>();
    private volatile boolean controlledReadsActive;
    private volatile boolean pacedRemoteWritesActive;
    private volatile long loadIterationStartNanos;

    @Override
    protected void afterSetup() throws Exception {
        validateDistributedParameters();
        openPeerListeners();
    }

    private void validateDistributedParameters() {
        if (remoteWritesPerSecond < 0) {
            throw new IllegalArgumentException("remoteWritesPerSecond cannot be negative");
        }
        if (remoteWritePlayerCount <= 0 || remoteWritePlayerCount > playerCount) {
            throw new IllegalArgumentException("remoteWritePlayerCount must be between 1 and playerCount");
        }
        if (listenerCount <= 0) {
            throw new IllegalArgumentException("listenerCount must be positive");
        }
        if (targetReadsPerSecond < 0) {
            throw new IllegalArgumentException("targetReadsPerSecond cannot be negative");
        }
    }

    private void openPeerListeners() throws SQLException {
        for (int i = 1; i < listenerCount; i++) {
            Properties properties = connectionProperties("static-data-benchmark-listener-");
            PGConnection connection = DriverManager.getConnection(postgresJdbcUrl(), properties)
                    .unwrap(PGConnection.class);
            connection.addNotificationListener(
                    "static-data-benchmark-peer-" + i,
                    "data_notification_v3",
                    new PGNotificationListener() {
                        @Override
                        public void notification(int processId, String channelName, String payload) {
                            peerNotifications.increment();
                        }
                    }
            );
            try (Statement statement = connection.createStatement()) {
                statement.execute("LISTEN data_notification_v3");
            }
            peerListeners.add(connection);
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        completedControlledReads.reset();
        completedPacedRemoteWrites.reset();
        peerNotifications.reset();
        controlledReadsActive = false;
        pacedRemoteWritesActive = false;
        loadIterationStartNanos = System.nanoTime();
        nextReadPermitNanos.set(loadIterationStartNanos);
    }

    @TearDown(Level.Iteration)
    public void reportLoadRates() {
        if (!controlledReadsActive && !pacedRemoteWritesActive) {
            return;
        }

        long elapsedNanos = System.nanoTime() - loadIterationStartNanos;
        StaticDataStatistics statistics = dataManager().getStatistics();
        if (controlledReadsActive) {
            System.out.printf(
                    "Controlled compound reads: target=%d/s, achieved=%.0f/s%n",
                    targetReadsPerSecond,
                    ratePerSecond(completedControlledReads.sum(), elapsedNanos)
            );
        }
        if (pacedRemoteWritesActive) {
            System.out.printf(
                    "Paced remote writes: per-peer target=%d/s, achieved aggregate=%.0f/s, " +
                            "notification-only peers=%d, observed fan-out=%.0f notifications/s%n",
                    remoteWritesPerSecond,
                    ratePerSecond(completedPacedRemoteWrites.sum(), elapsedNanos),
                    listenerCount - 1,
                    ratePerSecond(peerNotifications.sum(), elapsedNanos)
            );
        }
        System.out.printf(
                "Static Data rolling counters: H2 queries=%d/s, H2 updates=%d/s%n",
                statistics.getQueriesPerSecond(),
                statistics.getUpdatesPerSecond()
        );
    }

    private double ratePerSecond(long completed, long elapsedNanos) {
        return completed * (double) TimeUnit.SECONDS.toNanos(1) / elapsedNanos;
    }

    public long readRandomPlayerAtControlledRate() {
        awaitReadPermit();
        long result = readRandomPlayer();
        controlledReadsActive = true;
        completedControlledReads.increment();
        return result;
    }

    private void awaitReadPermit() {
        if (targetReadsPerSecond == 0) {
            return;
        }

        long intervalNanos = Math.max(1, TimeUnit.SECONDS.toNanos(1) / targetReadsPerSecond);
        long permitNanos = nextReadPermitNanos.getAndAdd(intervalNanos);
        while (true) {
            long remainingNanos = permitNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return;
            }
            if (remainingNanos > 5_000) {
                Thread.yield();
            } else {
                Thread.onSpinWait();
            }
        }
    }

    public RemoteWrite writeFromRemoteContainer(boolean paced, int concurrentWriters) {
        RemoteWriterSession session = writerSession.get();
        if (session == null) {
            session = openWriterSession(concurrentWriters);
            writerSession.set(session);
        } else if (session.concurrentWriters != concurrentWriters) {
            throw new IllegalStateException("Remote-writer concurrency changed within one trial");
        }

        if (paced) {
            session.awaitWritePermit(remoteWritesPerSecond);
        }
        RemoteWrite write = session.writeNext();
        if (paced) {
            pacedRemoteWritesActive = true;
            completedPacedRemoteWrites.increment();
        }
        return write;
    }

    private RemoteWriterSession openWriterSession(int concurrentWriters) {
        if (concurrentWriters <= 0 || concurrentWriters > remoteWritePlayerCount) {
            throw new IllegalArgumentException(
                    "concurrentWriters must be between 1 and remoteWritePlayerCount"
            );
        }

        int writerIndex = nextWriterIndex.getAndIncrement();
        if (writerIndex >= concurrentWriters) {
            throw new IllegalStateException(
                    "JMH created more remote-writer threads than the benchmark declared"
            );
        }

        try {
            Connection connection = DriverManager.getConnection(
                    postgresJdbcUrl(),
                    connectionProperties("static-data-benchmark-remote-")
            );
            RemoteWriterSession session = new RemoteWriterSession(
                    connection,
                    writerIndex,
                    concurrentWriters
            );
            writerSessions.add(session);
            return session;
        } catch (SQLException e) {
            throw new RuntimeException("Unable to open a simulated remote-container connection", e);
        }
    }

    public long awaitRemoteWrite(RemoteWrite write) {
        long start = System.nanoTime();
        long deadline = start + PROPAGATION_TIMEOUT_NANOS;
        int attempts = 0;

        while (!isVisible(write)) {
            if (System.nanoTime() >= deadline) {
                throw new RuntimeException(new TimeoutException(
                        "Remote write was not visible to the listening Static Data instance within 10 seconds"
                ));
            }

            if (attempts++ < 1_000) {
                Thread.onSpinWait();
            } else {
                Thread.yield();
            }
        }

        return System.nanoTime() - start;
    }

    private boolean isVisible(RemoteWrite write) {
        if (write.playerName()) {
            return Objects.equals(write.name(), playerAt(write.playerIndex()).name.get());
        }
        return write.priority() == settingsAt(write.playerIndex()).tablistPriority.get();
    }

    private Properties connectionProperties(String applicationNamePrefix) {
        Properties properties = new Properties();
        properties.setProperty("user", postgres().getUsername());
        properties.setProperty("password", postgres().getPassword());
        properties.setProperty("application.name", applicationNamePrefix + UUID.randomUUID());
        return properties;
    }

    private String postgresJdbcUrl() {
        return "jdbc:pgsql://" + postgres().getHost() + ':' +
                postgres().getFirstMappedPort() + '/' + postgres().getDatabaseName();
    }

    @Override
    protected void beforeTearDown() {
        RuntimeException closeFailure = null;

        for (RemoteWriterSession session : writerSessions) {
            try {
                session.close();
            } catch (RuntimeException e) {
                closeFailure = accumulate(closeFailure, e);
            }
        }
        writerSession.remove();

        for (PGConnection peerListener : peerListeners) {
            try {
                peerListener.close();
            } catch (SQLException e) {
                closeFailure = accumulate(
                        closeFailure,
                        new RuntimeException("Unable to close a simulated peer listener", e)
                );
            }
        }

        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private RuntimeException accumulate(RuntimeException current, RuntimeException next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }

    public record RemoteWrite(
            int playerIndex,
            boolean playerName,
            String name,
            int priority,
            long sequence
    ) {
    }

    private final class RemoteWriterSession implements AutoCloseable {
        private final int concurrentWriters;
        private final int firstPlayerIndex;
        private final int playerCount;
        private final Connection connection;
        private final PreparedStatement updatePlayerName;
        private final PreparedStatement updateSettingsPriority;
        private long sequence;
        private long nextWriteNanos;

        private RemoteWriterSession(
                Connection connection,
                int writerIndex,
                int concurrentWriters
        ) throws SQLException {
            this.connection = connection;
            this.concurrentWriters = concurrentWriters;
            this.firstPlayerIndex = remoteWritePlayerCount * writerIndex / concurrentWriters;
            int nextPartitionStart = remoteWritePlayerCount * (writerIndex + 1) / concurrentWriters;
            this.playerCount = nextPartitionStart - firstPlayerIndex;
            this.updatePlayerName = connection.prepareStatement(
                    "UPDATE \"skyblock\".\"players\" SET \"name\" = ? WHERE \"id\" = ?"
            );
            this.updateSettingsPriority = connection.prepareStatement(
                    "UPDATE \"skyblock\".\"player_settings\" SET \"tablist_priority\" = ? WHERE \"id\" = ?"
            );
        }

        private void awaitWritePermit(int writesPerSecond) {
            if (writesPerSecond == 0) {
                return;
            }

            long interval = TimeUnit.SECONDS.toNanos(1) / writesPerSecond;
            long now = System.nanoTime();
            if (nextWriteNanos == 0 || now - nextWriteNanos > interval) {
                nextWriteNanos = now;
            }

            long waitNanos = nextWriteNanos - now;
            if (waitNanos > 0) {
                LockSupport.parkNanos(waitNanos);
            }
            nextWriteNanos += interval;
        }

        private RemoteWrite writeNext() {
            long currentSequence = ++sequence;
            boolean playerName = (currentSequence & 1) == 1;
            int playerIndex = firstPlayerIndex + Math.floorMod(currentSequence - 1, playerCount);

            try {
                if (playerName) {
                    String name = "RemotePlayer-" + playerIndex + '-' + currentSequence;
                    updatePlayerName.setString(1, name);
                    updatePlayerName.setObject(2, playerIdAt(playerIndex));
                    requireOneUpdatedRow(updatePlayerName.executeUpdate());
                    return new RemoteWrite(playerIndex, true, name, 0, currentSequence);
                }

                int priority = Math.toIntExact(currentSequence);
                updateSettingsPriority.setInt(1, priority);
                updateSettingsPriority.setObject(2, settingsIdAt(playerIndex));
                requireOneUpdatedRow(updateSettingsPriority.executeUpdate());
                return new RemoteWrite(playerIndex, false, null, priority, currentSequence);
            } catch (SQLException e) {
                throw new RuntimeException("Remote-container write failed", e);
            }
        }

        private void requireOneUpdatedRow(int updatedRows) {
            if (updatedRows != 1) {
                throw new IllegalStateException("Expected one updated row, got " + updatedRows);
            }
        }

        @Override
        public void close() {
            try {
                updatePlayerName.close();
                updateSettingsPriority.close();
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Unable to close a simulated remote-container connection", e);
            }
        }
    }
}
