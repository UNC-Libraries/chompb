package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Options for boxctron file mapping
 * @author krwong
 */
public class BoxctronFileMappingOptions {
    @Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, then the output of the matching will be displayed in the console rather "
                            + "than written to file"})
    private boolean dryRun;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, then any source file matches produced will be used to update an existing"
                            + " source file mapping file, instead of attempting to create a new one.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = { "-f", "--force"},
            description = "Overwrite mapping file if one already exists")
    private boolean force;

    public boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean getUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
