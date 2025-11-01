package net.staticstudios.data.query.clause;

import java.util.Collections;
import java.util.List;

public class OrClause implements ConditionalClause {

    @Override
    public List<Object> append(StringBuilder sb) {
        sb.append(" OR ");
        return Collections.emptyList();
    }
}
