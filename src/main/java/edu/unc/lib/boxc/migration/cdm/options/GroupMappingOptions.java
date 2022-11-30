package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Options for generating object grouping mappings
 *
 * @author bbpennel
 */
public class GroupMappingOptions {

    @Option(names = {"-n", "--field-name"},
            description = {
                    "Name of the CDM export field to perform grouping on."},
            defaultValue = "file")
    private String groupField;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, new groupings will be merged into an existing mapping file.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, then the output of the grouping will be displayed in the console rather "
                    + "than written to file"})
    private boolean dryRun;

    @Option(names = { "-f", "--force"},
            description = "Overwrite mapping file if one already exists")
    private boolean force;

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public boolean getUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean getForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
