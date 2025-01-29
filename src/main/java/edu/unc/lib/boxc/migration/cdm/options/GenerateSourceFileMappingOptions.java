package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Options for generate file mapping
 * @author krwong
 */
public class GenerateSourceFileMappingOptions extends SourceFileMappingOptions implements GenerateFileMappingOptions {
    @Option(names = {"-g", "--glob-pattern"},
            description = {
                    "Optional pattern for adjusting which files within the base path to search.",
                    "Must be relative to the base path. Follows glob pattern syntax.",
                    "For example, to only match tiff files within the base path:",
                    "    *.tiff",
                    "To match any file within any directory at a depth of 2 subdirectories:",
                    "    */*/*",
                    "Or to match tiff files at any depth:",
                    "    **/*.tiff"})
    private String pathPattern;

    @Option(names = {"-n", "--field-name"},
            description = {
                    "Name of the CDM export field which will be transformed by the field matching pattern "
                            + "to produce the source file filename for matching purposes."},
            defaultValue = "file")
    private String exportField;

    @Option(names = {"-p", "--field-pattern"},
            description = {
                    "Regular expression which will be used to extract portions of the export field value "
                            + "for use in the filename template. Use matching groups for this.",
                    "Must match the entire value of the export field.",
                    "For example, to extract numeric portions of the value: 276_214_E.tif",
                    "You could provide the pattern: (\\d+)\\_(\\d+)_E.tif"},
            defaultValue = "(.+)")
    private String fieldMatchingPattern;

    @Option(names = {"-t", "--file-template"},
            description = {
                    "Template used to produce expected source file filenames.",
                    "It should be used with matching groups from --field-pattern.",
                    "NOTE: Use single quotes to wrap this value, or escape the $ characters as \\$.",
                    "Given the field pattern above, it could be templated out to: 00276_op0214_0001_e.tif",
                    "With the template: 00$1_op0$2_0001_e.tif"},
            defaultValue = "$1")
    private String filenameTemplate;

    @Option(names = {"-l", "--lower-template"},
            description = "Convert the filename produced from the --file-temp option to lowercase "
                    + "prior to attempting to match against source files.")
    private boolean lowercaseTemplate;

    @Option(names = { "-B", "--blank"},
            description = "Populate a blank source mapping file. Entries will be added for each object as per normal,"
                    + " but without attempting to map them to any files")
    private boolean populateBlank;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, then any source file matches produced will be used to update an existing"
                            + " source file mapping file, instead of attempting to create a new one.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = { "-f", "--force"},
            description = "Overwrite mapping file if one already exists")
    private boolean force;

    public String getPathPattern() {
        return pathPattern;
    }

    public void setPathPattern(String pathPattern) {
        this.pathPattern = pathPattern;
    }

    public String getExportField() {
        return exportField;
    }

    public void setExportField(String exportField) {
        this.exportField = exportField;
    }

    public String getFieldMatchingPattern() {
        return fieldMatchingPattern;
    }

    public void setFieldMatchingPattern(String fieldMatchingPattern) {
        this.fieldMatchingPattern = fieldMatchingPattern;
    }

    public String getFilenameTemplate() {
        return filenameTemplate;
    }

    public void setFilenameTemplate(String filenameTemplate) {
        this.filenameTemplate = filenameTemplate;
    }

    public boolean isLowercaseTemplate() {
        return lowercaseTemplate;
    }

    public void setLowercaseTemplate(boolean lowercaseTemplate) {
        this.lowercaseTemplate = lowercaseTemplate;
    }

    @Override
    public boolean getUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean isPopulateBlank() {
        return populateBlank;
    }

    public void setPopulateBlank(boolean populateBlank) {
        this.populateBlank = populateBlank;
    }
}
