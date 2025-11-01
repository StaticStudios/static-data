package net.staticstudios.data.query.clause;

import java.util.Collections;
import java.util.List;

public class AndClause implements ConditionalClause {

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append(" AND ");
        return Collections.emptyList();
    }
}
