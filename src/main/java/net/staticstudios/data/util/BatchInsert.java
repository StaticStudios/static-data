package net.staticstudios.data.util;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.data.InitialValue;
import org.jetbrains.annotations.Blocking;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Represents a batch of insertions to be performed.
 * This is especially useful when foreign key constraints are involved, as it allows for all insertions to be performed
 * in a single transaction.
 */
public class BatchInsert {
    private final List<InsertContext> insertionContexts = new CopyOnWriteArrayList<>();
    private final List<Runnable> postInsertActions = new CopyOnWriteArrayList<>();
    private final List<Runnable> preInsertActions = new CopyOnWriteArrayList<>();
    private final List<Predicate<BatchInsert>> preconditions = new CopyOnWriteArrayList<>();
    private final List<ConnectionConsumer> intermediateActions = new CopyOnWriteArrayList<>();
    private final DataManager dataManager;
    private transient boolean flushed = false;

    /**
     * Create a new batch of insertions.
     *
     * @param dataManager The data manager to use for the insertions
     */
    public BatchInsert(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    /**
     * Add a new insertion to the batch.
     *
     * @param holder      The holder of the data to be inserted
     * @param initialData The initial data to be inserted
     */
    public void add(UniqueData holder, InitialValue<?, ?>... initialData) {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }
        InsertContext context = dataManager.buildInsertContext(holder, initialData);
        insertionContexts.add(context);
    }

    public void post(Runnable action) {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }
        postInsertActions.add(action);
    }

    public void intermediate(ConnectionConsumer action) {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }
        intermediateActions.add(action);
    }

    public void early(Runnable action) {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }
        preInsertActions.add(action);
    }

    public void precondition(Predicate<BatchInsert> precondition) {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }
        preconditions.add(precondition);
    }

    /**
     * Get the insertion contexts in this batch.
     *
     * @return The insertion contexts
     */
    public List<InsertContext> getInsertionContexts() {
        return insertionContexts;
    }

    /**
     * Asynchronously insert all data in this batch.
     */
    public void insertAsync() {
        updateFlushed();

        for (Predicate<BatchInsert> precondition : preconditions) {
            if (!precondition.test(this)) {
                throw new IllegalStateException("Precondition failed, batch insertion aborted");
            }
        }

        dataManager.insertBatchAsync(this, intermediateActions, preInsertActions, postInsertActions);
    }

    /**
     * Synchronously insert all data in this batch.
     */
    @Blocking
    public void insert() {
        updateFlushed();

        for (Predicate<BatchInsert> precondition : preconditions) {
            if (!precondition.test(this)) {
                throw new IllegalStateException("Precondition failed, batch insertion aborted");
            }
        }

        dataManager.insertBatch(this, intermediateActions, preInsertActions, postInsertActions);
    }

    private synchronized void updateFlushed() {
        if (flushed) {
            throw new IllegalStateException("BatchInsertion has already been flushed");
        }

        flushed = true;
    }
}
