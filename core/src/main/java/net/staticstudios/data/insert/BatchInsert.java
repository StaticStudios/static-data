package net.staticstudios.data.insert;

import com.google.common.base.Preconditions;
import net.staticstudios.data.DataManager;
import net.staticstudios.data.InsertMode;

import java.util.ArrayList;
import java.util.List;

public class BatchInsert {
    private final DataManager dataManager;
    private final List<InsertContext> insertContexts = new ArrayList<>();
    private final List<PostInsertAction> postInsertActions = new ArrayList<>();

    public BatchInsert(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    public List<PostInsertAction> getPostInsertActions() {
        return postInsertActions;
    }

    public void addPostInsertAction(PostInsertAction action) {
        Preconditions.checkNotNull(action, "PostInsertAction cannot be null");
        postInsertActions.add(action);
    }

    public void add(InsertContext context) {
        Preconditions.checkNotNull(context, "InsertContext cannot be null");
        Preconditions.checkState(!insertContexts.contains(context), "InsertContext already added to BatchInsert");
        insertContexts.add(context);
    }

    public void insert(InsertMode insertMode) {
        Preconditions.checkNotNull(insertMode, "InsertMode cannot be null");
        Preconditions.checkState(!insertContexts.isEmpty(), "No InsertContexts to insert");
        Preconditions.checkArgument(insertContexts.stream().noneMatch(InsertContext::isInserted), "All InsertContexts must not be inserted before calling insert");
        dataManager.insert(this, insertMode);
    }

    public List<InsertContext> getInsertContexts() {
        return insertContexts;
    }
}
