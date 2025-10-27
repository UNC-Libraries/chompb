package edu.unc.lib.boxc.migration.cdm;

import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;

import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.assertDateIndexedNotPresent;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.assertDateIndexedPresent;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.mappingBody;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.setExportedDate;
import static edu.unc.lib.boxc.migration.cdm.test.IndexServiceHelper.writeCsv;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author bbpennel
 */
public class CdmIndexCommandIT extends AbstractCommandIT {
    @Test
    public void indexGilmerTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/descriptions/gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getIndexPath()));
        assertDateIndexedPresent(project);
    }

    @Test
    public void indexAlreadyExistsTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/descriptions/mini_gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);
        assertTrue(Files.exists(project.getIndexPath()));
        long indexSize = Files.size(project.getIndexPath());

        // Index a second time, should fail and index should be unchanged
        executeExpectFailure(args);
        assertOutputContains("Cannot create index, an index file already exists");
        assertTrue(Files.exists(project.getIndexPath()));
        assertEquals(indexSize, Files.size(project.getIndexPath()));
        assertDateIndexedPresent(project);

        // Add more data and index again with force flag
        Files.copy(Paths.get("src/test/resources/descriptions/gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project), StandardCopyOption.REPLACE_EXISTING);

        String[] argsForce = new String[] {
                "-w", project.getProjectPath().toString(),
                "index",
                "-f"};
        executeExpectSuccess(argsForce);
        assertTrue(Files.exists(project.getIndexPath()));
        assertNotEquals(indexSize, Files.size(project.getIndexPath()), "Index should have changed size");
        assertDateIndexedPresent(project);
    }

    @Test
    public void indexingFailureTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());

        FileUtils.write(CdmFileRetrievalService.getDescAllPath(project).toFile(), "uh oh", ISO_8859_1);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectFailure(args);
        assertOutputContains("Failed to parse desc.all file");
        assertDateIndexedNotPresent(project);
        assertTrue(Files.notExists(project.getIndexPath()), "Index file should be cleaned up");
    }

    @Test
    public void indexWithWarningsTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.createDirectories(CdmFileRetrievalService.getExportedCpdsPath(project));
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/image/620.cpd"),
                CdmFileRetrievalService.getExportedCpdsPath(project).resolve("620.cpd"));
        Files.copy(Paths.get("src/test/resources/keepsakes_fields.csv"), project.getFieldsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getIndexPath()));
        assertDateIndexedPresent(project);

        assertOutputContains("CPD file referenced by object 604 in desc.all was not found");
    }

    @Test
    public void indexFromCsvTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());

        Files.copy(Paths.get("src/test/resources/files/exported_objects.csv"), project.getExportObjectsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index",
                "-c", "src/test/resources/files/exported_objects.csv"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getIndexPath()));
        assertTrue(Files.exists(project.getFieldsPath()));
        assertDateIndexedPresent(project);
    }

    @Test
    public void indexFromEadToCdmTsvTest() throws Exception {
        initProject();
        Files.createDirectories(project.getExportPath());
        // make source_files.csv
        writeCsv(project, mappingBody("00001,," + project.getProjectPath() + "/00011_0045_0001.tif,",
                "00002,," + project.getProjectPath() + "/00011_0045_0002.tif,"));

        Files.copy(Paths.get("src/test/resources/files/ead_to_cdm.tsv"), project.getExportObjectsPath());
        setExportedDate(project);

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index",
                "-ead", "src/test/resources/files/ead_to_cdm.tsv"};
        executeExpectSuccess(args);

        assertTrue(Files.exists(project.getIndexPath()));
        assertTrue(Files.exists(project.getFieldsPath()));
        assertDateIndexedPresent(project);
    }

    @Test
    public void indexFromCsvAndEadToCdmTsvTest() throws Exception {
        initProject();

        String[] args = new String[] {
                "-w", project.getProjectPath().toString(),
                "index",
                "-c", "src/test/resources/files/exported_objects.csv",
                "-ead", "src/test/resources/files/ead_to_cdm.tsv"};
        executeExpectFailure(args);
        assertOutputContains("CSVs and EAD to CDM TSVs may not be used in the same indexing command");
        assertDateIndexedNotPresent(project);
        assertTrue(Files.notExists(project.getIndexPath()), "Index file should be cleaned up");
    }
}
