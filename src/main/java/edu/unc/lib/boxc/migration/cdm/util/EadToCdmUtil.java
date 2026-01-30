package edu.unc.lib.boxc.migration.cdm.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.ArrayUtils;

import static edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CDM_ID;

/**
 * Helper methods and constants related to EAD to CDM indexing and exporting
 * @author snluong
 */
public class EadToCdmUtil {
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

    public static final String STANDARDIZED_COLLECTION_NAME = standardizeHeader(COLLECTION_NAME);
    public static final String STANDARDIZED_COLLECTION_NUMBER = standardizeHeader(COLLECTION_NUMBER);
    public static final String STANDARDIZED_LOC_IN_COLLECTION = standardizeHeader(LOC_IN_COLLECTION);
    public static final String STANDARDIZED_CITATION = standardizeHeader(CITATION);
    public static final String STANDARDIZED_FILENAME = standardizeHeader(FILENAME);
    public static final String STANDARDIZED_OBJ_FILENAME = standardizeHeader(OBJ_FILENAME);
    public static final String STANDARDIZED_CONTAINER_TYPE = standardizeHeader(CONTAINER_TYPE);
    public static final String STANDARDIZED_HOOK_ID = standardizeHeader(HOOK_ID);
    public static final String STANDARDIZED_OBJECT = standardizeHeader(OBJECT);
    public static final String STANDARDIZED_COLLECTION_URL = standardizeHeader(COLLECTION_URL);
    public static final String STANDARDIZED_GENRE_FORM = standardizeHeader(GENRE_FORM);
    public static final String STANDARDIZED_EXTENT = standardizeHeader(EXTENT);
    public static final String STANDARDIZED_UNIT_DATE = standardizeHeader(UNIT_DATE);
    public static final String STANDARDIZED_GEOGRAPHIC_NAME = standardizeHeader(GEOGRAPHIC_NAME);
    public static final String STANDARDIZED_REF_ID = standardizeHeader(REF_ID);
    public static final String STANDARDIZED_PROCESS_INFO = standardizeHeader(PROCESS_INFO);
    public static final String STANDARDIZED_SCOPE_CONTENT = standardizeHeader(SCOPE_CONTENT);
    public static final String STANDARDIZED_UNIT_TITLE = standardizeHeader(UNIT_TITLE);
    public static final String STANDARDIZED_CONTAINER = standardizeHeader(CONTAINER);
    public static String[] TSV_HEADERS = new String[] { COLLECTION_NAME, COLLECTION_NUMBER, LOC_IN_COLLECTION, CITATION,
            FILENAME, OBJ_FILENAME, CONTAINER_TYPE, HOOK_ID, OBJECT, COLLECTION_URL, GENRE_FORM, EXTENT, UNIT_DATE,
            GEOGRAPHIC_NAME, REF_ID, PROCESS_INFO, SCOPE_CONTENT, UNIT_TITLE, CONTAINER };

    public static String[] TSV_STANDARDIZED_HEADERS = new String[] {
            STANDARDIZED_COLLECTION_NAME,
            STANDARDIZED_COLLECTION_NUMBER,
            STANDARDIZED_LOC_IN_COLLECTION,
            STANDARDIZED_CITATION,
            STANDARDIZED_FILENAME,
            STANDARDIZED_OBJ_FILENAME,
            STANDARDIZED_CONTAINER_TYPE,
            STANDARDIZED_HOOK_ID,
            STANDARDIZED_OBJECT,
            STANDARDIZED_COLLECTION_URL,
            STANDARDIZED_GENRE_FORM,
            STANDARDIZED_EXTENT,
            STANDARDIZED_UNIT_DATE,
            STANDARDIZED_GEOGRAPHIC_NAME,
            STANDARDIZED_REF_ID,
            STANDARDIZED_PROCESS_INFO,
            STANDARDIZED_SCOPE_CONTENT,
            STANDARDIZED_UNIT_TITLE,
            STANDARDIZED_CONTAINER
    };

    // standardized tsv headers with CDM ID included
    public static String[] TSV_WITH_ID_HEADERS = ArrayUtils.addAll(TSV_STANDARDIZED_HEADERS, CDM_ID);

    /**
     * Formats string from EAD to CDM JSON API response
     * @param key standardized header name
     * @param jsonNode json node array
     * @return empty string or string without quotation marks
     */
    public static String getValue(String key, JsonNode jsonNode) {
        var value = jsonNode.get(key);
        if (value == null) {
            return "";
        }
        return value.asText();
    }

    public static String standardizeHeader(String header) {
        return header.replaceAll(" ", "_").replaceAll("\"", "").toLowerCase();
    }

    private EadToCdmUtil() {
    }
}
