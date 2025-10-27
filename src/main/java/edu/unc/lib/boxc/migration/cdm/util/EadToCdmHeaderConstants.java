package edu.unc.lib.boxc.migration.cdm.util;

import static edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CDM_ID;

/**
 * Constants related to the EAD to CDM TSV used for indexing
 */
public class EadToCdmHeaderConstants {
    public static final String COLLECTION_NAME = "Collection name";
    public static final String COLLECTION_NUMBER = "Collection Number";
    public static final String LOC_IN_COLLECTION = "Location in collection";
    public static final String CITATION = "Citation";
    public static final String FILENAME = "Filename";
    public static final String OBJ_FILENAME = "Object Filename";
    public static final String CONTAINER_TYPE = "Container Type";
    public static final String HOOK_ID = "Hook ID";
    public static final String OBJECT = "Object";
    public static final String COLLECTION_URL = "Collection URL";
    public static final String GENRE_FORM = "Genre Form";
    public static final String EXTENT = "Extent";
    public static final String UNIT_DATE = "Unit Date";
    public static final String GEOGRAPHIC_NAME = "Geographic Name";
    public static final String REF_ID = "Ref ID";
    public static final String PROCESS_INFO = "processinfo";
    public static final String SCOPE_CONTENT = "scopecontent";
    public static final String UNIT_TITLE = "unitTitle";
    public static final String CONTAINER = "container";
    public static String[] TSV_HEADERS = new String[] { COLLECTION_NAME, COLLECTION_NUMBER, LOC_IN_COLLECTION, CITATION,
            FILENAME, OBJ_FILENAME, CONTAINER_TYPE, HOOK_ID, OBJECT, COLLECTION_URL, GENRE_FORM, EXTENT, UNIT_DATE,
            GEOGRAPHIC_NAME, REF_ID, PROCESS_INFO, SCOPE_CONTENT, UNIT_TITLE, CONTAINER };

    // new standardized tsv headers with CDM ID included
    public static String[] TSV_WITH_ID_HEADERS = new String[] {
            standardizeHeader(COLLECTION_NAME),
            standardizeHeader(COLLECTION_NUMBER),
            standardizeHeader(LOC_IN_COLLECTION),
            standardizeHeader(CITATION),
            standardizeHeader(FILENAME),
            standardizeHeader(OBJ_FILENAME),
            standardizeHeader(CONTAINER_TYPE),
            standardizeHeader(HOOK_ID),
            standardizeHeader(OBJECT),
            standardizeHeader(COLLECTION_URL),
            standardizeHeader(GENRE_FORM),
            standardizeHeader(EXTENT),
            standardizeHeader(UNIT_DATE),
            standardizeHeader(GEOGRAPHIC_NAME),
            standardizeHeader(REF_ID),
            standardizeHeader(PROCESS_INFO),
            standardizeHeader(SCOPE_CONTENT),
            standardizeHeader(UNIT_TITLE),
            standardizeHeader(CONTAINER),
            CDM_ID
    };

    public static String standardizeHeader(String header) {
        return header.replaceAll(" ", "_").replaceAll("\"", "").toLowerCase();
    }

    private EadToCdmHeaderConstants() {
    }
}
