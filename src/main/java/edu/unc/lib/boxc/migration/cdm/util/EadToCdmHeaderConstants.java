package edu.unc.lib.boxc.migration.cdm.util;

import static edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CDM_ID;

public class EadToCdmHeaderConstants {
    public static final String COLLECTION_NAME = "collection_name";
    public static final String COLLECTION_NUMBER = "collection_number";
    public static final String LOC_IN_COLLECTION = "location_in_collection";
    public static final String CITATION = "citation";
    public static final String FILENAME = "filename";
    public static final String OBJ_FILENAME = "object_filename";
    public static final String CONTAINER_TYPE = "container_type";
    public static final String HOOK_ID = "hook_id";
    public static final String OBJECT = "object";
    public static final String COLLECTION_URL = "collection_url";
    public static final String REF_ID = "ref_id";
    public static String[] TSV_HEADERS = new String[] { COLLECTION_NAME, COLLECTION_NUMBER, LOC_IN_COLLECTION, CITATION,
            FILENAME, OBJ_FILENAME, CONTAINER_TYPE, HOOK_ID, OBJECT, COLLECTION_URL, REF_ID, CDM_ID };
    public static String[] TSV_WITH_ID_HEADERS = new String[] { COLLECTION_NAME, COLLECTION_NUMBER, LOC_IN_COLLECTION, CITATION,
        FILENAME, OBJ_FILENAME, CONTAINER_TYPE, HOOK_ID, OBJECT, COLLECTION_URL, REF_ID, CDM_ID };
}
