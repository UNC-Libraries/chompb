package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

import java.util.List;

/**
 * Options for filtering the cdm index to a subset of the original items
 * @author bbpennel
 */
public class IndexFilteringOptions {
    @CommandLine.Option(names = {"-n", "--field-name"},
            description = {
                    "Name of the CDM export field to use for filtering."},
            defaultValue = "descri")
    private String fieldName;

    @CommandLine.Option(names = {"-i", "--include"},
            description = {
                    "Filter the index to only records which match the provided value(s)."})
    private List<String> includeValues;

    @CommandLine.Option(names = {"-e", "--exclude"},
            description = {
                    "Filter the index to only records which DO NOT match the provided value(s)."})
    private List<String> excludeValues;

    @CommandLine.Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, only output how many items would be left if the filter were applied."})
    private boolean dryRun;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> getIncludeValues() {
        return includeValues;
    }

    public void setIncludeValues(List<String> includeValues) {
        this.includeValues = includeValues;
    }

    public List<String> getExcludeValues() {
        return excludeValues;
    }

    public void setExcludeValues(List<String> excludeValues) {
        this.excludeValues = excludeValues;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
