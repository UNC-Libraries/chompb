package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.IndexFilteringOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class IndexFilteringServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    @TempDir
    public Path tmpFolder;
    private SipServiceHelper testHelper;
    private MigrationProject project;
    private IndexFilteringService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME,
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        testHelper = new SipServiceHelper(project, tmpFolder);
        service = new IndexFilteringService();
        service.setProject(project);
        service.setIndexService(testHelper.getIndexService());
    }

    @Test
    public void calculateRemainderIncludeSingleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setIncludeValues(Arrays.asList("group1"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 2);
    }

    @Test
    public void calculateRemainderIncludeMultipleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        // Some of the records have blank groups, so lets match on those
        options.setIncludeValues(Arrays.asList("group1", ""));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 3);
    }

    @Test
    public void calculateRemainderIncludeAllValuesTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        // Some of the records have blank groups, so lets match on those
        options.setIncludeValues(Arrays.asList("group1", "group2", ""));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 4);
    }

    @Test
    public void calculateRemainderIncludeNoMatchesTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setIncludeValues(Arrays.asList("what!!"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 0);
    }

    @Test
    public void calculateRemainderExcludeSingleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setExcludeValues(Arrays.asList("group1"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 3);
    }

    @Test
    public void calculateRemainderExcludeMultipleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setExcludeValues(Arrays.asList("group1", "group2"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 5);
        assertEquals(result.get("remainder"), 2);
    }

    @Test
    public void calculateRemainderIncludeCompoundAndRegularTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new IndexFilteringOptions();
        options.setFieldName("subjed");
        options.setIncludeValues(Arrays.asList("Chapel Hill (N.C.)"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 7);
        assertEquals(result.get("remainder"), 4);
    }

    @Test
    public void calculateRemainderExcludeCompoundAndRegularTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new IndexFilteringOptions();
        options.setFieldName("subjed");
        options.setExcludeValues(Arrays.asList("Chapel Hill (N.C.)"));
        var result = service.calculateRemainder(options);
        assertEquals(result.get("total"), 7);
        assertEquals(result.get("remainder"), 3);
    }

    @Test
    public void filterIndexIncludeSingleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setIncludeValues(Arrays.asList("group1"));
        service.filterIndex(options);

        var remaining = getRemainingIds();
        assertTrue(remaining.contains("25"));
        assertTrue(remaining.contains("26"));
        assertEquals(2, remaining.size());
    }

    @Test
    public void filterIndexIncludeMultipleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        // Some of the records have blank groups, so lets match on those
        options.setIncludeValues(Arrays.asList("group1", ""));
        service.filterIndex(options);
        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("25", "26", "29"), remaining);
    }


    @Test
    public void filterIndexExcludeSingleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setExcludeValues(Arrays.asList("group1"));
        service.filterIndex(options);
        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("27", "28", "29"), remaining);
    }

    @Test
    public void filterIndexExcludeMultipleValueTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("groupa");
        options.setExcludeValues(Arrays.asList("group1", "group2"));
        service.filterIndex(options);
        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("28", "29"), remaining);
    }

    @Test
    public void filterIndexIncludeCompoundAndRegularTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new IndexFilteringOptions();
        options.setFieldName("subjed");
        options.setIncludeValues(Arrays.asList("Chapel Hill (N.C.)"));
        service.filterIndex(options);
        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("216", "605", "606", "607"), remaining);
    }

    @Test
    public void filterIndexExcludeCompoundAndRegularTest() throws Exception {
        testHelper.indexExportData("mini_keepsakes");

        var options = new IndexFilteringOptions();
        options.setFieldName("subjed");
        options.setExcludeValues(Arrays.asList("Chapel Hill (N.C.)"));
        service.filterIndex(options);
        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("602", "603", "604"), remaining);
    }

    @Test
    public void filterIndexIncludeRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("dmcreated");
        options.setIncludeRangeStart("2005-12-01");
        options.setIncludeRangeEnd("2005-12-31");
        service.filterIndex(options);

        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("27", "28", "29"), remaining);
        assertEquals(3, remaining.size());
    }

    @Test
    public void filterIndexExcludeRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");

        var options = new IndexFilteringOptions();
        options.setFieldName("dmcreated");
        options.setExcludeRangeStart("2005-12-01");
        options.setExcludeRangeEnd("2005-12-31");
        service.filterIndex(options);

        var remaining = getRemainingIds();
        assertIterableEquals(Arrays.asList("25", "26"), remaining);
        assertEquals(2, remaining.size());
    }

    private List<String> getRemainingIds() throws Exception {
        Connection conn = testHelper.getIndexService().openDbConnection();
        var result = new ArrayList<String>();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID +
                    " from " + CdmIndexService.TB_NAME +
                    " order by " + CdmFieldInfo.CDM_ID + " asc");
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        return result;
    }
}
