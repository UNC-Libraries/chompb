package edu.unc.lib.boxc.migration.cdm.validators;

import java.nio.file.Path;

/**
 * Validator for aggregate file mappings
 *
 * @author bbpennel
 */
public class AggregateFilesValidator extends SourceFilesValidator {
    private boolean sortBottom;

    public AggregateFilesValidator(boolean sortBottom) {
        this.sortBottom = sortBottom;
    }

    @Override
    protected Path getMappingPath() {
        if (sortBottom) {
            return project.getAggregateBottomMappingPath();
        } else {
            return project.getAggregateTopMappingPath();
        }
    }

    @Override
    protected boolean allowUnmapped() {
        return true;
    }
}
