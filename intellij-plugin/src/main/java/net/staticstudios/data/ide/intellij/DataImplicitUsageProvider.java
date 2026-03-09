package net.staticstudios.data.ide.intellij;

import com.intellij.codeInsight.daemon.ImplicitUsageProvider;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiField;
import net.staticstudios.data.utils.Constants;
import org.jetbrains.annotations.NotNull;

public class DataImplicitUsageProvider implements ImplicitUsageProvider {

    @Override
    public boolean isImplicitUsage(@NotNull PsiElement psiElement) {
        return false;
    }

    @Override
    public boolean isImplicitRead(@NotNull PsiElement psiElement) {
        return false;
    }

    @Override
    public boolean isImplicitWrite(@NotNull PsiElement psiElement) {
        if (!(psiElement instanceof PsiField field)) {
            return false;
        }

        return IntelliJPluginUtils.hasAnnotationRecursive(field, Constants.IMPLICIT_WRITE_FQN);
    }

}
