package net.staticstudios.data.query;

import com.google.common.base.Preconditions;
import net.staticstudios.data.query.clause.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

@SuppressWarnings("unused")
public abstract class BaseQueryWhere {
    protected final Set<InnerJoin> innerJoins = new HashSet<>();
    private final Stack<Node> nonGrouped = new Stack<>();
    private Node root = null;

    private static void buildWhereClauseRecursive(Node node, StringBuilder sb, List<Object> parameters) {
        if (node == null) {
            return;
        }
        boolean isConditional = node.clause instanceof ConditionalClause;
        if (isConditional) {
            sb.append("(");
        }

        buildWhereClauseRecursive(node.lhs, sb, parameters);
        List<Object> clauseParams = node.clause.append(sb);
        parameters.addAll(clauseParams);
        buildWhereClauseRecursive(node.rhs, sb, parameters);
        if (isConditional) {
            sb.append(")");
        }
    }

    protected void addInnerJoin(String referringSchema, String referringTable, String[] columnsInReferringTable, String referencedSchema, String referencedTable, String[] columnsInReferencedTable) {
        innerJoins.add(new InnerJoin(referringSchema, referringTable, columnsInReferringTable, referencedSchema, referencedTable, columnsInReferencedTable));
    }

    protected boolean isEmpty() {
        return root == null;
    }

    protected Set<InnerJoin> getInnerJoins() {
        return innerJoins;
    }

    /**
     * Push the current state onto the stack to start a new group.
     */
    protected void pushGroup() {
        if (root != null) {
            Preconditions.checkState(root.clause instanceof ConditionalClause && root.rhs == null, "Invalid state! Cannot start a group here!");
        }
        nonGrouped.push(root);
        root = null;
    }

    /**
     * Pop the last group from the stack and set it as the new root.
     * Setting the current root as the right-hand side of the popped group.
     */
    protected void popGroup() {
        Preconditions.checkState(!nonGrouped.isEmpty(), "Invalid state! No group to pop!");
        Node newRoot = nonGrouped.pop();
        if (newRoot == null) {
            return;
        }

        newRoot.rhs = root;
        root = newRoot;
    }

    protected void andClause() {
        setConditionalClause(new AndClause());
    }

    protected void orClause() {
        setConditionalClause(new OrClause());
    }

    protected void equalsClause(String schema, String table, String column, @Nullable Object o) {
        if (o == null) {
            nullClause(schema, table, column);
            return;
        }
        setValueClause(new EqualsClause(schema, table, column, o));
    }

    protected void notEqualsClause(String schema, String table, String column, @Nullable Object o) {
        if (o == null) {
            notNullClause(schema, table, column);
            return;
        }
        setValueClause(new NotEqualsClause(schema, table, column, o));
    }

    //todo: add IJ and processor support for equalsIgnoreCaseClause and notEqualsIgnoreCaseClause
    protected void equalsIgnoreCaseClause(String schema, String table, String column, @NotNull String eq) {
        setValueClause(new EqualsIngoreCaseClause(schema, table, column, eq));
    }

    protected void notEqualsIgnoreCaseClause(String schema, String table, String column, @NotNull String neq) {
        setValueClause(new NotEqualsIngoreCaseClause(schema, table, column, neq));
    }

    protected void nullClause(String schema, String table, String column) {
        setValueClause(new NullClause(schema, table, column));
    }

    protected void notNullClause(String schema, String table, String column) {
        setValueClause(new NotNullClause(schema, table, column));
    }

    protected void likeClause(String schema, String table, String column, String format) {
        setValueClause(new LikeClause(schema, table, column, format));
    }

    protected void notLikeClause(String schema, String table, String column, String format) {
        setValueClause(new NotLikeClause(schema, table, column, format));
    }

    protected void inClause(String referringSchema, String referringTable, String column, Object[] in) {
        setValueClause(new InClause(referringSchema, referringTable, column, in));
    }

    protected void notInClause(String referringSchema, String referringTable, String column, Object[] in) {
        setValueClause(new NotInClause(referringSchema, referringTable, column, in));
    }

    protected void betweenClause(String schema, String table, String column, Object min, Object max) {
        setValueClause(new BetweenClause(schema, table, column, min, max));
    }

    protected void notBetweenClause(String schema, String table, String column, Object min, Object max) {
        setValueClause(new NotBetweenClause(schema, table, column, min, max));
    }

    protected void greaterThanClause(String schema, String table, String column, Object o) {
        setValueClause(new GreaterThanClause(schema, table, column, o));
    }

    protected void greaterThanOrEqualToClause(String schema, String table, String column, Object o) {
        setValueClause(new GreaterThanOrEqualToClause(schema, table, column, o));
    }

    protected void lessThanClause(String schema, String table, String column, Object o) {
        setValueClause(new LessThanClause(schema, table, column, o));
    }

    protected void lessThanOrEqualToClause(String schema, String table, String column, Object o) {
        setValueClause(new LessThanOrEqualToClause(schema, table, column, o));
    }

    private void setConditionalClause(Clause clause) {
        Preconditions.checkState(root != null, "Invalid state! Cannot set conditional clause '" + clause + "' here!");
        if (root.clause instanceof ConditionalClause) {
            Node lhs = root.lhs;
            Node rhs = root.rhs;
            Preconditions.checkState(lhs != null && rhs != null, "Invalid state! Cannot set conditional clause '" + clause + "' here!");
        }
        Node newRoot = new Node();
        newRoot.clause = clause;
        newRoot.lhs = root;
        root = newRoot;
    }

    private void setValueClause(Clause clause) {
        if (root == null) {
            root = new Node();
            root.clause = clause;
        } else {
            Preconditions.checkState(root.rhs == null, "Invalid state! Cannot set clause '" + clause + "' here!");
            root.rhs = new Node();
            root.rhs.clause = clause;
        }
    }

    public void buildWhereClause(StringBuilder sb, List<Object> parameters) {
        buildWhereClauseRecursive(root, sb, parameters);
    }

    static class Node {
        Clause clause;
        Node lhs;
        Node rhs;
    }
}
