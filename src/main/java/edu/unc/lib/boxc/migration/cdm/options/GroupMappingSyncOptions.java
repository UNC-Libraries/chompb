package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for operation to sync group mappings back into the database
 * @author bbpennel
 */
public class GroupMappingSyncOptions {
    @CommandLine.Option(names = {"-n", "--sort-field"},
            description = {
                    "Name of the CDM export field to sort grouped files by within each grouped work."},
            defaultValue = "file")
    private String sortField;

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }
}
