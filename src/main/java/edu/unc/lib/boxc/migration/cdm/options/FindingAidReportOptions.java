package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Options for finding aid reports
 * @author krwong
 */
public class FindingAidReportOptions {
    @Option(names = {"-f", "--field"},
            description = {"Field to generate finding aid field report for. Defaults to descri"},
            defaultValue = "descri")
    private String field;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
