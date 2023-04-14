package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.IndexFilteringOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    // include
    // exclude
    // single value
    // multiple values
    // returns nothing
    // filter
    // remainder
    // with compounds

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
        assertEquals(result.get("remainder"), 4);
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
        assertEquals(result.get("remainder"), 5);
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
}
