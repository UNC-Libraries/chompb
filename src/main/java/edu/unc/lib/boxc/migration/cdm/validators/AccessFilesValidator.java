package edu.unc.lib.boxc.migration.cdm.validators;

import java.nio.file.Path;

/**
 * Validator for access file mappings
 *
 * @author bbpennel
 */
public class AccessFilesValidator extends SourceFilesValidator {

    public AccessFilesValidator() {
    }

    @Override
    protected Path getMappingPath() {
        return project.getAccessFilesMappingPath();
    }
}
