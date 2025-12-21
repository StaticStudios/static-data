package net.staticstudios.data.ide.intellij.query;

import com.intellij.psi.PsiField;
import com.intellij.psi.PsiType;
import net.staticstudios.data.ide.intellij.IntelliJPluginUtils;
import net.staticstudios.data.ide.intellij.query.clause.*;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class QueryBuilderUtils {
    private static final List<QueryClause> pvClauses;
    private static final List<QueryClause> referenceClauses;

    static {
        pvClauses = new ArrayList<>();
        pvClauses.add(new IsClause());
        pvClauses.add(new IsNotClause());

        pvClauses.add(new IsInCollectionClause());
        pvClauses.add(new IsNotInCollectionClause());

        pvClauses.add(new IsInArrayClause());
        pvClauses.add(new IsNotInArrayClause());

        pvClauses.add(new IsNullClause());
        pvClauses.add(new IsNotNullClause());

        pvClauses.add(new IsLikeClause());
        pvClauses.add(new IsNotLikeClause());

        pvClauses.add(new IsIgnoreCaseClause());
        pvClauses.add(new IsNotIgnoreCaseClause());

        pvClauses.add(new IsGreaterThanClause());
        pvClauses.add(new IsLessThanClause());
        pvClauses.add(new IsGreaterThanOrEqualToClause());
        pvClauses.add(new IsLessThanOrEqualToClause());
        pvClauses.add(new IsBetweenClause());
        pvClauses.add(new IsNotBetweenClause());

        referenceClauses = new ArrayList<>();
        //todo: supporting these clauses in the java-c plugin is more involved than pvs, so until those are implemented
        // these will remain diables. at the time of writing this, uncommenting this will cause IJ to behave as expected.
//        referenceClauses.add(new IsClause());
//        referenceClauses.add(new IsNotClause());
//        referenceClauses.add(new IsNullClause());
//        referenceClauses.add(new IsNotNullClause());
    }

    public static List<QueryClause> getClausesForType(PsiField psiField, boolean nullable) {
        if (IntelliJPluginUtils.isValidPersistentValue(psiField)) {
            List<QueryClause> applicableClauses = new ArrayList<>();
            for (QueryClause clause : pvClauses) {
                if (clause.matches(psiField, nullable)) {
                    applicableClauses.add(clause);
                }
            }
            return applicableClauses;
        }
        if (IntelliJPluginUtils.isValidReference(psiField)) {
            List<QueryClause> applicableClauses = new ArrayList<>();
            for (QueryClause clause : referenceClauses) {
                if (clause.matches(psiField, nullable)) {
                    applicableClauses.add(clause);
                }
            }
            return applicableClauses;
        }
        return List.of();
    }

    public static boolean isNumeric(PsiType psiType) {
        String typeName = psiType.getCanonicalText();
        return typeName.equals(Integer.class.getName()) ||
                typeName.equals(Long.class.getName()) ||
                typeName.equals(Float.class.getName()) ||
                typeName.equals(Double.class.getName()) ||
                typeName.equals(Short.class.getName()) ||
                typeName.equals(Timestamp.class.getName()) ||
                typeName.equals("int") ||
                typeName.equals("long") ||
                typeName.equals("float") ||
                typeName.equals("short") ||
                typeName.equals("double");
    }
}
