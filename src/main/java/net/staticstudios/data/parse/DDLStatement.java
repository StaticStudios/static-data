package net.staticstudios.data.parse;

public record DDLStatement(String h2Statement, String postgresqlStatement) {

    public static DDLStatement both(String statement) {
        return new DDLStatement(statement, statement);
    }

    public static DDLStatement of(String h2Statement, String postgresqlStatement) {
        return new DDLStatement(h2Statement, postgresqlStatement);
    }
}
