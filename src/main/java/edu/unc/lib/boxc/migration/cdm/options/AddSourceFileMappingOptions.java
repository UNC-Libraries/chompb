package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

import java.util.List;

/**
 * Options for add to file mapping
 * @author krwong
 */
public class AddSourceFileMappingOptions extends SourceFileMappingOptions {
    @Option(names = {"-e", "--extensions"},
            description = {"Provide list of file extensions to include in the source mapping file. Defaults to tif"},
            defaultValue = "tif",
            converter = LowerCaseConverter.class)
    private List<String> extensions;

    @Option(names = {"-p", "--id-prefix"},
            description = "Provide an optional prefix for IDs within the migration project. " +
                    "These IDs will only be used for work in chompb.")
    private String optionalIdPrefix;

    public List<String> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<String> extensions) {
        this.extensions = extensions;
    }

    public String getOptionalIdPrefix() {
        return optionalIdPrefix;
    }

    public void setOptionalIdPrefix(String optionalIdPrefix) {
        this.optionalIdPrefix = optionalIdPrefix;
    }
}
