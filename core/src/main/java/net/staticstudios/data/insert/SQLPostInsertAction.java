package net.staticstudios.data.insert;

import net.staticstudios.data.util.SQlStatement;

import java.util.ArrayList;
import java.util.List;

public class SQLPostInsertAction implements PostInsertAction {

    private final List<SQlStatement> statements = new ArrayList<>();


    public void addStatement(SQlStatement statement) {
        statements.add(statement);
    }

    @Override
    public List<SQlStatement> getStatements() {
        return statements;
    }
}
