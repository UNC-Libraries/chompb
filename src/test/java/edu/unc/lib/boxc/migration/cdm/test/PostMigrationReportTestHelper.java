package edu.unc.lib.boxc.migration.cdm.test;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.util.PostMigrationReportConstants;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Reader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper.TEST_BASE_URL;
import static edu.unc.lib.boxc.migration.cdm.util.PostMigrationReportConstants.RECORD_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author bbpennel
 */
public class PostMigrationReportTestHelper {
    private PostMigrationReportTestHelper() {
    }
    public static final String PARENT_COLL_ID = "4fe5080f-41cd-4b1e-9cdd-71203c824cd0";
    public static final String PARENT_COLL_URL = TEST_BASE_URL + RECORD_PATH + PARENT_COLL_ID;
    public static final String PARENT_COLL_TITLE = "Latin Studies Program";
    private static final String SIP_ID = "6f4b5e38-754f-49ca-a4a0-6441fea95d76";
    public static final String JSON = "{\"findingAidUrl\":\"https://finding-aids.lib.unc.edu/catalog/40489\"," +
            "\"viewerType\":\"clover\",\"canBulkDownload\":false,\"dataFileUrl\":\"content/6f4b5e38-754f-49ca-a4a0-6441fea95d76\"," +
            "\"markedForDeletion\":false,\"pageSubtitle\":\"TEST.jpg\",\"briefObject\":{\"added\":\"2018-05-24T20:39:18.165Z\"," +
            "\"counts\":{\"child\":1},\"created\":\"2018-05-24T20:39:18.165Z\",\"format\":[\"Image\"],\"parentCollectionName\":\"" + PARENT_COLL_TITLE +"\"," +
            "\"contentStatus\":[\"Not Described\",\"Has Primary Object\"],\"rollup\":\"1a1e9c1a-cdd2-4874-b6cb-8da783919460\"," +
            "\"parentCollectionId\":\"" + PARENT_COLL_ID + "\",\"id\":\"1a1e9c1a-cdd2-4874-b6cb-8da783919460\"," +
            "\"updated\":\"2018-05-25T13:37:01.864Z\",\"fileType\":[\"image/jpeg\"],\"status\":[\"Public Access\"],\"timestamp\":1751312648385}," +
            "\"collectionId\":\"40489\",\"resourceType\":\"Work\"}";

    public static List<List<String>> parseReport(MigrationProject project) throws Exception {
        try (
                Reader reader = Files.newBufferedReader(project.getPostMigrationReportPath());
                CSVParser csvParser = new CSVParser(reader, PostMigrationReportConstants.CSV_PARSER_FORMAT);
        ) {
            var rows = new ArrayList<List<String>>();
            for (CSVRecord csvRecord : csvParser) {
                rows.add(csvRecord.toList());
            }
            return rows;
        }
    }

    // Assert row matches, without verifying the bxc urls in case the ids are unknown
    public static void assertContainsRow(List<List<String>> rows, String cdmId, String cdmUrl, String objType,
                                         String bxcTitle, String matchingValue, String sourceFile,
                                         String verified, String parentTitle, String childCount) {
        var found = rows.stream().filter(r -> r.get(0).equals(cdmId)).findFirst().orElse(null);
        assertNotNull(found, "Did not find row for CDM id" + cdmId);
        assertEquals(cdmUrl, found.get(1));
        assertEquals(objType, found.get(2));
        assertEquals(bxcTitle, found.get(4));
        assertEquals(matchingValue, found.get(5));
        assertEquals(sourceFile, found.get(6));
        assertEquals(verified, found.get(7));
        assertEquals(parentTitle, found.get(9));
        assertEquals(childCount, found.get(10));
    }

    // Assert row matches all provided fields
    public static void assertContainsRow(List<List<String>> rows, String cdmId, String cdmUrl, String objType,
                                         String bxcUrl, String bxcTitle, String matchingValue, String sourceFile,
                                         String verified, String parentUrl, String parentTitle, String childCount,
                                         String sipId, String parentCollUrl, String parentCollTitle ) {
        var found = rows.stream().filter(r -> r.get(0).equals(cdmId)).findFirst().orElse(null);
        assertNotNull(found, "Did not find row for CDM id " + cdmId + ", rows were:\n" + rows);
        assertEquals(Arrays.asList(cdmId, cdmUrl, objType, bxcUrl, bxcTitle, matchingValue, sourceFile, verified,
                parentUrl, parentTitle, childCount, sipId, parentCollUrl, parentCollTitle), found);
    }
}
