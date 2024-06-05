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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author bbpennel
 */
public class PostMigrationReportTestHelper {
    private PostMigrationReportTestHelper() {
    }

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
                                         String verified, String parentUrl, String parentTitle, String childCount) {
        var found = rows.stream().filter(r -> r.get(0).equals(cdmId)).findFirst().orElse(null);
        assertNotNull(found, "Did not find row for CDM id " + cdmId + ", rows were:\n" + rows);
        assertEquals(Arrays.asList(cdmId, cdmUrl, objType, bxcUrl, bxcTitle, matchingValue, sourceFile, verified,
                parentUrl, parentTitle, childCount), found);
    }
}
