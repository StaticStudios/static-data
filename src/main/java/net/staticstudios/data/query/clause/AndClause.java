package net.staticstudios.data.query.clause;

import java.util.ArrayList;
import java.util.List;

public class AndClause implements CompositeClause {
    private final Clause left;
    private final Clause right;

    public AndClause(Clause left, Clause right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        List<Object> values = new ArrayList<>(left.append(sb));
        sb.append(" AND ");
        values.addAll(right.append(sb));

        return values;
    }
}
