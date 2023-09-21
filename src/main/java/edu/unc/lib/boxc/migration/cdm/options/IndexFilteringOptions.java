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

    @CommandLine.Option(names = {"-is", "--include-range-start"},
            description = {
                    "Filter the index to records AFTER the provided START value. Inclusive of start/end values."})
    private String includeRangeStart;

    @CommandLine.Option(names = {"-ie", "--include-range-end"},
            description = {
                    "Filter the index to records BEFORE the provided END value. Inclusive of start/end values."})
    private String includeRangeEnd;

    @CommandLine.Option(names = {"-es", "--exclude-range-start"},
            description = {
                    "Filter the index to records BEFORE the provided START value. Exclusive of start/end values."})
    private String excludeRangeStart;

    @CommandLine.Option(names = {"-ee", "--exclude-range-end"},
            description = {
                    "Filter the index to records which are AFTER the provided END value. Exclusive of start/end values."})
    private String excludeRangeEnd;

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

    public String getIncludeRangeStart() {
        return includeRangeStart;
    }

    public void setIncludeRangeStart(String includeRangeStart) {
        this.includeRangeStart = includeRangeStart;
    }

    public String getIncludeRangeEnd() {
        return includeRangeEnd;
    }

    public void setIncludeRangeEnd(String includeRangeEnd) {
        this.includeRangeEnd = includeRangeEnd;
    }

    public String getExcludeRangeStart() {
        return excludeRangeStart;
    }

    public void setExcludeRangeStart(String excludeRangeStart) {
        this.excludeRangeStart = excludeRangeStart;
    }

    public String getExcludeRangeEnd() {
        return excludeRangeEnd;
    }

    public void setExcludeRangeEnd(String excludeRangeEnd) {
        this.excludeRangeEnd = excludeRangeEnd;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
