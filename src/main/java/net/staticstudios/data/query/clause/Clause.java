package net.staticstudios.data.query.clause;

import java.util.List;

public interface Clause {

    List<Object> append(StringBuilder sb);
}
