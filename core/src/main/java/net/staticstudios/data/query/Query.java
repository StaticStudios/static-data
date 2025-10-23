package net.staticstudios.data.query;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.query.clause.Clause;

public class Query<T extends UniqueData> {
    private final Clause clause;

    protected Query(Clause clause) {
        this.clause = clause;
    }
}
