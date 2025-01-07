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

    public Path getAltTextCsvFile() {
        return altTextCsvFile;
    }

    public void setAltTextCsvFile(Path altTextCsvFile) {
        this.altTextCsvFile = altTextCsvFile;
    }
}
