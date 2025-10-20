package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author bbpennel
 */
public class FilterIndexCommandIT extends AbstractCommandIT {
    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
    }

    @Test
    public void filterNotIndexedTest() throws Exception {
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "25"};
        executeExpectFailure(args);

        assertOutputContains("Project must be indexed");
    }

    @Test
    public void filterNoFieldNameTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "",
                "-i", "group1"};
        executeExpectFailure(args);

        assertOutputContains("Must provide a --field-name value");
    }

    @Test
    public void filterIncludeOrExcludeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa"};
        executeExpectFailure(args);

        assertOutputContains("Must provide an --include, --exclude, --include-range-start " +
                "and --include-range-end, or --exclude-range-start and --exclude range-end value(s) (but not all)");
    }

    @Test
    public void filterBothIncludeAndExcludeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "group1",
                "-e", "group2"};
        executeExpectFailure(args);

        assertOutputContains("Cannot provide both --include and --exclude at the same time");
    }

    @Test
    public void filterResultingInNoEntriesTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "OHNO"};
        executeExpectFailure(args);

        assertOutputContains("Filter would remove all entries from the index");
        assertRemaining(5);
    }

    @Test
    public void filterResultingInNoChangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "type",
                "-i", "Maps"};
        executeExpectFailure(args);

        assertOutputContains("Number of entries in the index (5) would be unchanged by the filter");
        assertRemaining(5);
    }

    @Test
    public void filterSingleIncludeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "group1"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 2 remaining entries");
        assertOutputContains("Filtering of index for my_proj completed");
        assertRemaining(2);
    }

    @Test
    public void filterMultipleIncludeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "group1",
                "-i", "group2"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 3 remaining entries");
        assertOutputContains("Filtering of index for my_proj completed");
        assertRemaining(3);
    }

    @Test
    public void filterMultipleExcludeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-e", "group1",
                "-e", ""};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 2 remaining entries");
        assertOutputContains("Filtering of index for my_proj completed");
        assertRemaining(2);
    }

    @Test
    public void filterIncludeRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "dmcreated",
                "-is", "2005-12-01",
                "-ie", "2005-12-31"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 3 remaining entries");
        assertOutputContains("Filtering of index for my_proj completed");
        assertRemaining(3);
    }

    @Test
    public void filterExcludeRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "dmcreated",
                "-es", "2005-12-01",
                "-ee", "2005-12-31"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 2 remaining entries");
        assertOutputContains("Filtering of index for my_proj completed");
        assertRemaining(2);
    }

    @Test
    public void filterBothIncludeExcludeRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "dmcreated",
                "-is", "2005-12-01",
                "-ie", "2005-12-31",
                "-es", "2005-12-01",
                "-ee", "2005-12-31"};
        executeExpectFailure(args);

        assertOutputContains("Cannot provide both --include-range and --exclude-range values at the same time");
    }

    @Test
    public void filterNoIncludeStartRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-ie", "2005-12-01",};
        executeExpectFailure(args);

        assertOutputContains("Must provide both --include-range-start and --include-range-end");
    }

    @Test
    public void filterNoExcludeEndRangeTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-es", "2005-12-01"};
        executeExpectFailure(args);

        assertOutputContains("Must provide both --exclude-range-start and --exclude-range-end");
    }

    @Test
    public void filterIncludeDryRunTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-i", "group1",
                "--dry-run"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 2 remaining entries");
        assertOutputContains("Dry run, no entries have been removed from the index");
        assertRemaining(5);
    }

    @Test
    public void filterExcludeDryRunTest() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "filter_index",
                "-n", "groupa",
                "-e", "group1",
                "--dry-run"};
        executeExpectSuccess(args);

        assertOutputContains("Filtering index from 5 to 3 remaining entries");
        assertOutputContains("Dry run, no entries have been removed from the index");
        assertRemaining(5);
    }

    private void assertRemaining(int expected) throws Exception {
        assertEquals(expected, countRemaining(), "Incorrect number of entries remaining in index");
    }

    private int countRemaining() throws Exception {
        Connection conn = testHelper.getCdmIndexService().openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            rs.next();
            return rs.getInt(1);
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }
}
