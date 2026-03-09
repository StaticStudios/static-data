package net.staticstudios.data.ide.intellij;

import com.intellij.codeInsight.daemon.ImplicitUsageProvider;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiField;
import net.staticstudios.data.utils.Constants;
import org.jetbrains.annotations.NotNull;

public class DataImplicitUsageProvider implements ImplicitUsageProvider {
    @Override
    public boolean isImplicitUsage(@NotNull PsiElement psiElement) {
        return checkAnnotation(psiElement);
    }

    @Override
    public boolean isImplicitRead(@NotNull PsiElement psiElement) {
        return checkAnnotation(psiElement);
    }

    @Override
    public boolean isImplicitWrite(@NotNull PsiElement psiElement) {
        return checkAnnotation(psiElement);
    }

    private boolean checkAnnotation(PsiElement psiElement) {
        if (!(psiElement instanceof PsiField field)) {
            return false;
        }

        return IntelliJPluginUtils.hasAnnotation(field, Constants.COLUMN_ANNOTATION_FQN)
                || IntelliJPluginUtils.hasAnnotation(field, Constants.FOREIGN_COLUMN_ANNOTATION_FQN)
                || IntelliJPluginUtils.hasAnnotation(field, Constants.ID_COLUMN_ANNOTATION_FQN);
    }
}
