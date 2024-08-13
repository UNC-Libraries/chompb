package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

import java.nio.file.Path;

/**
 * Options for indexing object records
 * @author krwong
 */
public class CdmIndexOptions {
    @Option(names = {"-c", "--from-csv"},
    description = {"Export objects CSV file used as source for populating sqlite database."})
    private Path csvFile;

    public Path getCsvFile() {
        return csvFile;
    }

    public void setCsvFile(Path csvFile) {
        this.csvFile = csvFile;
    }
}
