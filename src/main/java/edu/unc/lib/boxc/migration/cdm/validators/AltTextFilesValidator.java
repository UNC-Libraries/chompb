package edu.unc.lib.boxc.migration.cdm.validators;

import java.nio.file.Path;

/**
 * Validator for alt-text file mappings
 * @author krwong
 */
public class AltTextFilesValidator extends SourceFilesValidator {
    public AltTextFilesValidator() {
    }

    @Override
    protected Path getMappingPath() {
        return project.getAltTextFilesMappingPath();
    }
}
