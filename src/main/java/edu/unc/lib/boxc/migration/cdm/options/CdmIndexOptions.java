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

    @Option(names = { "-f", "--force"},
            description = "Overwrite index if one already exists")
    private boolean force;

    public Path getCsvFile() {
        return csvFile;
    }

    public void setCsvFile(Path csvFile) {
        this.csvFile = csvFile;
    }

    public boolean getForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
