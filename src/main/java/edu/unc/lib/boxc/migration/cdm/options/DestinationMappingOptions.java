package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.Option;

/**
 * Destination mapping options
 * @author bbpennel
 */
public class DestinationMappingOptions {
    @Option(names = {"-id", "--cdm-id"},
            description = {
                    "CDM ID(s) going to a specific box-c deposit destination in this migration project.",
                    "Must be a CDM ID or a comma-delimited list of CDM IDs"})
    private String cdmId;

    @Option(names = {"-dd", "--default-dest"},
            description = {
                    "Default box-c deposit destination for objects in this migration project.",
                    "The default will be used during SIP generation for objects if they do not have explicit mappings.",
                    "Must be a UUID or other valid PID format."})
    private String defaultDestination;

    @Option(names = {"-dc", "--default-coll"},
            description = {
                    "Default collection id for objects in this migration project.",
                    "The default will be used during SIP generation for objects if they do not have explicit mappings.",
                    "If this is populated, a new collection will be created and used "
                            + "as the destination for objects mapped to the default destination.",
                    "Can be any value that uniquely identifies objects that belong in this destination.",})
    private String defaultCollection;

    @Option(names = {"-n", "--field-name"},
            description = {"Name of the field in CDM where archival collection numbers should be found.",
                    "The default will be used during SIP generation for objects if they do not have explicit mappings.",})
    private String fieldName;

    @Option(names = {"-f", "--force"},
            description = "Overwrite destination mapping if one already exists")
    private boolean force;

    @Option(names = {"-ac", "--archival-collections"},
            description = "Generate the destination mappings file by matching CDM field values to " +
                    "the archival collection number in boxc")
    private boolean archivalCollections;

    public String getDefaultDestination() {
        return defaultDestination;
    }

    public void setDefaultDestination(String defaultDestination) {
        this.defaultDestination = defaultDestination;
    }

    public String getDefaultCollection() {
        return defaultCollection;
    }

    public void setDefaultCollection(String defaultCollection) {
        this.defaultCollection = defaultCollection;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public String getCdmId() {
        return cdmId;
    }

    public void setCdmId(String cdmId) {
        this.cdmId = cdmId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public boolean isArchivalCollections() {
        return archivalCollections;
    }

    public void setArchivalCollections(boolean archivalCollections) {
        this.archivalCollections = archivalCollections;
    }
}
