package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

import java.nio.file.Path;

/**
 * Options for boxctron file mapping
 * @author krwong
 */
public class BoxctronFileMappingOptions implements GenerateFileMappingOptions {
    @Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, then the output of the matching will be displayed in the console rather "
                            + "than written to file"})
    private boolean dryRun;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, then any boxctron access file matches produced will be used to update an existing"
                            + " access file mapping file, instead of attempting to create a new one.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = { "-f", "--force"},
            description = "Overwrite mapping file if one already exists")
    private boolean force;

    @Option(names = {"-e", "--exclusions-csv"},
    description = {"Provide a csv with exclusions. Access file mappings will be skipped for these items."})
    private Path exclusionsCsv;

    @Override
    public boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
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

    public Path getExclusionsCsv() {
        return exclusionsCsv;
    }

    public void setExclusionsCsv(Path exclusionsCsv) {
        this.exclusionsCsv = exclusionsCsv;
    }
}
