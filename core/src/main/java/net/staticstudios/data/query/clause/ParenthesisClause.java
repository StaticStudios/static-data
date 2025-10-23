package net.staticstudios.data.query.clause;

import java.util.List;

public class ParenthesisClause implements ValueClause {
    private final Clause inner;

    public ParenthesisClause(Clause clause) {
        this.inner = clause;
    }

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append("(");
        List<Object> values = inner.append(sb);
        sb.append(")");
        return values;
    }
}
