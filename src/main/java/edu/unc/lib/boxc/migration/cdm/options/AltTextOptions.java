package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.List;

/**
 * Options for alt-text
 * @author krwong
 */
public class AltTextOptions {
    @Option(names = {"-fc", "--from-csv"},
            description = {"Alt-text CSV file to upload"})
    private Path altTextCsvFile;

    @Option(names = {"-ft", "--from-txt"},
            split = ",",
            description = {"Alt-text txt files to upload"})
    private List<String> altTextTxtFiles;

    public Path getAltTextCsvFile() {
        return altTextCsvFile;
    }

    public void setAltTextCsvFile(Path altTextCsvFile) {
        this.altTextCsvFile = altTextCsvFile;
    }

    public List<String> getAltTextTxtFiles() {
        return altTextTxtFiles;
    }

    public void setAltTextTxtFiles(List<String> altTextTxtFiles) {
        this.altTextTxtFiles = altTextTxtFiles;
    }
}
