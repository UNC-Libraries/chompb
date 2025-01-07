package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

import java.util.List;

/**
 * Options for alt-text file mapping
 * @author krwong
 */
public class AltTextFileMappingOptions extends GenerateSourceFileMappingOptions {
    @Option(names = {"-e", "--extensions"},
            split = ",",
            description = {"Provide list of file extensions to include in the alt-text mapping file. Defaults to txt"},
            defaultValue = "txt",
            converter = LowerCaseConverter.class)
    private List<String> extensions;

    public List<String> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<String> extensions) {
        this.extensions = extensions;
    }
}
