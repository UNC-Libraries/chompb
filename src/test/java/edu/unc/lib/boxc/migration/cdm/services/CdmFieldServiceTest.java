package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import edu.unc.lib.boxc.migration.cdm.model.ExportObjectsInfo;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo.CdmFieldEntry;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author bbpennel
 */
public class CdmFieldServiceTest {
    private static final String CDM_BASE_URL = "http://example.com:88/";
    private static final String PROJECT_NAME = "proj";
    private AutoCloseable closeable;
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService service;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse httpResp;
    @Mock
    private HttpEntity respEntity;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        project = MigrationProjectFactory.createCdmMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user",
                CdmEnvironmentHelper.DEFAULT_ENV_ID, BxcEnvironmentHelper.DEFAULT_ENV_ID);
        service = new CdmFieldService();
        service.setHttpClient(httpClient);
        service.setCdmBaseUri(CDM_BASE_URL);
        service.setProject(project);

        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResp);
        when(httpResp.getEntity()).thenReturn(respEntity);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void retrieveCdmFieldsTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/cdm_fields_resp.json"));

        CdmFieldInfo fieldInfo = service.retrieveFieldsForCollection(PROJECT_NAME);
        List<CdmFieldEntry> fields = fieldInfo.getFields();

        assertHasFieldWithValue("title", "title", "Title", false,
                "y", "y", "n", "n",
                "title", fields);
        assertHasFieldWithValue("creato", "creato", "Creator", false,
                "n", "y", "n", "y",
                "creato", fields);
        assertHasFieldWithValue("date", "date", "Creation Date", false,
                "n", "y", "y", "n",
                "datea", fields);
        assertHasFieldWithValue("data", "data", "Date", false,
                "n", "n", "n", "n",
                "date", fields);
        assertHasFieldWithValue("digitb", "digitb", "Digital Collection",
                false, "n", "y", "n",
                "n", "BLANK", fields);
    }

    @Test
    public void retrieveCdmFieldsInvalidResponseTest() throws Exception {
        when(respEntity.getContent()).thenReturn(new ByteArrayInputStream("well that's not right".getBytes()));

        try {
            service.retrieveFieldsForCollection(PROJECT_NAME);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Failed to parse response from URL"));
        }
    }

    @Test
    public void retrieveCdmCollectionNotFoundTest() throws Exception {
        when(respEntity.getContent()).thenReturn(new ByteArrayInputStream(
                ("Error looking up collection /" + PROJECT_NAME + "<br>\n").getBytes()));
        try {
            service.retrieveFieldsForCollection(PROJECT_NAME);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("No collection with ID '" + PROJECT_NAME),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void retrieveCdmFieldsIncorrectStructureTest() throws Exception {
        when(respEntity.getContent()).thenReturn(new ByteArrayInputStream("{ 'oh' : 'no' }".getBytes()));

        try {
            service.retrieveFieldsForCollection(PROJECT_NAME);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Unexpected response from URL"));
            assertTrue(e.getMessage().contains("It must be a JSON array"));
        }
    }

    @Test
    public void retrieveCdmFieldsConnectionFailureTest() throws Exception {
        Assertions.assertThrows(IOException.class, () -> {
            when(httpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Fail"));

            service.retrieveFieldsForCollection(PROJECT_NAME);
        });
    }

    @Test
    public void persistEmptyFieldsTest() throws Exception {
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        service.persistFieldsToProject(project, fieldInfo);
        assertTrue(Files.size(project.getFieldsPath()) > 0, "Fields file must not be empty");
        String contents = FileUtils.readFileToString(project.getFieldsPath().toFile(), StandardCharsets.UTF_8);
        assertEquals(String.join(",", CdmFieldService.EXPORT_CSV_HEADERS), contents.trim());
    }

    @Test
    public void persistAndLoadFieldsTest() throws Exception {
        CdmFieldInfo fieldInfoOriginal = new CdmFieldInfo();
        List<CdmFieldEntry> fieldsOriginal = fieldInfoOriginal.getFields();
        CdmFieldEntry field1 = new CdmFieldEntry();
        field1.setNickName("field1");
        field1.setExportAs("field1");
        field1.setDescription("First field");
        field1.setSkipExport(false);
        field1.setCdmRequired("y");
        field1.setCdmSearchable("y");
        field1.setCdmHidden("y");
        field1.setCdmVocab("y");
        field1.setCdmDcMapping("field1");
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field2");
        field2.setExportAs("secondary");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        field2.setCdmRequired("n");
        field2.setCdmSearchable("n");
        field2.setCdmHidden("n");
        field2.setCdmVocab("n");
        field2.setCdmDcMapping("field2");
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        assertTrue(Files.size(project.getFieldsPath()) > 0, "Fields file must not be empty");

        CdmFieldInfo fieldInfoLoaded = service.loadFieldsFromProject(project);
        List<CdmFieldEntry> fieldsLoaded = fieldInfoLoaded.getFields();
        assertEquals(2, fieldsLoaded.size());
        assertHasFieldWithValue("field1", "field1", "First field", false,
                "y", "y", "y", "y",
                "field1", fieldsLoaded);
        assertHasFieldWithValue("field2", "secondary", "Secondary Field", true,
                "n", "n", "n", "n",
                "field2", fieldsLoaded);
    }

    @Test
    public void validateMissingFieldsFileTest() throws Exception {
        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("CDM fields file is missing"));
        }
    }

    @Test
    public void validateEmptyFieldsFileTest() throws Exception {
        FileUtils.write(project.getFieldsPath().toFile(), "", StandardCharsets.UTF_8);

        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("CDM fields file is empty"));
        }
    }

    @Test
    public void validateDuplicateNickTest() throws Exception {
        CdmFieldInfo fieldInfoOriginal = new CdmFieldInfo();
        List<CdmFieldEntry> fieldsOriginal = fieldInfoOriginal.getFields();
        CdmFieldEntry field1 = new CdmFieldEntry();
        field1.setNickName("field1");
        field1.setExportAs("field1");
        field1.setDescription("First field");
        field1.setSkipExport(false);
        field1.setCdmRequired("y");
        field1.setCdmSearchable("y");
        field1.setCdmHidden("y");
        field1.setCdmVocab("y");
        field1.setCdmDcMapping("field1");
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field1");
        field2.setExportAs("secondary");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        field2.setCdmRequired("n");
        field2.setCdmSearchable("n");
        field2.setCdmHidden("n");
        field2.setCdmVocab("n");
        field2.setCdmDcMapping("field2");
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Duplicate cdm_nick value 'field1' at line 3"),
                    "Unexpected error message " + e.getMessage());
        }
    }

    @Test
    public void validateDuplicateExportAsTest() throws Exception {
        CdmFieldInfo fieldInfoOriginal = new CdmFieldInfo();
        List<CdmFieldEntry> fieldsOriginal = fieldInfoOriginal.getFields();
        CdmFieldEntry field1 = new CdmFieldEntry();
        field1.setNickName("field1");
        field1.setExportAs("field1");
        field1.setDescription("First field");
        field1.setSkipExport(false);
        field1.setCdmRequired("y");
        field1.setCdmSearchable("y");
        field1.setCdmHidden("y");
        field1.setCdmVocab("y");
        field1.setCdmDcMapping("field1");
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field2");
        field2.setExportAs("field1");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        field2.setCdmRequired("n");
        field2.setCdmSearchable("n");
        field2.setCdmHidden("n");
        field2.setCdmVocab("n");
        field2.setCdmDcMapping("field2");
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Duplicate export_as value 'field1' at line 3"),
                    "Unexpected error message " + e.getMessage());
        }
    }

    @Test
    public void validateInvalidFieldsFileTest() throws Exception {
        FileUtils.write(project.getFieldsPath().toFile(), "this,is,not,really\nfields,file,but,lets\nkeep,it,going",
                StandardCharsets.UTF_8);

        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Invalid CDM fields entry at line 2"));
        }
    }

    @Test
    public void loadMissingFieldsFileTest() throws Exception {
        Assertions.assertThrows(InvalidProjectStateException.class, () -> {
            service.loadFieldsFromProject(project);
        });
    }

    @Test
    public void retrieveValidateAndReloadRoundTripTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/cdm_fields_resp.json"));

        CdmFieldInfo fieldInfoRetrieved = service.retrieveFieldsForCollection(PROJECT_NAME);
        service.persistFieldsToProject(project, fieldInfoRetrieved);

        service.validateFieldsFile(project);

        CdmFieldInfo fieldInfoLoaded = service.loadFieldsFromProject(project);
        List<CdmFieldEntry> fieldsLoaded = fieldInfoLoaded.getFields();

        assertHasFieldWithValue("title", "title", "Title", false,
                "y", "y", "n", "n",
                "title", fieldsLoaded);
        assertHasFieldWithValue("creato", "creato", "Creator", false,
                "n", "y", "n", "y",
                "creato", fieldsLoaded);
        assertHasFieldWithValue("date", "date", "Creation Date", false,
                "n", "y", "y", "n",
                "datea", fieldsLoaded);
        assertHasFieldWithValue("data", "data", "Date", false,
                "n", "n", "n", "n",
                "date", fieldsLoaded);
        assertHasFieldWithValue("digitb", "digitb", "Digital Collection", false,
                "n", "y", "n", "n",
                "BLANK", fieldsLoaded);
    }

    @Test
    public void retrieveCdmFieldsFromCsvTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/files/exported_objects.csv"),
                project.getExportObjectsPath());

        CdmFieldInfo fieldInfo = service.retrieveFieldsFromCsv(Paths.get("src/test/resources/files/exported_objects.csv"));
        List<CdmFieldEntry> fields = fieldInfo.getFields();

        assertHasFieldWithValue(ExportObjectsInfo.RECORD_ID, ExportObjectsInfo.RECORD_ID, ExportObjectsInfo.RECORD_ID,
                false, null, null, null,
                null, null, fields);
        assertHasFieldWithValue(ExportObjectsInfo.FILE_PATH, ExportObjectsInfo.FILE_PATH, ExportObjectsInfo.FILE_PATH,
                false, null, null, null,
                null, null, fields);
        assertHasFieldWithValue(ExportObjectsInfo.FILENAME, ExportObjectsInfo.FILENAME, ExportObjectsInfo.FILENAME,
                false, null, null, null,
                null, null, fields);
    }

    @Test
    public void persistAndLoadFromCsvFieldsTest() throws Exception {
        CdmFieldInfo fieldInfoOriginal = new CdmFieldInfo();
        List<CdmFieldEntry> fieldsOriginal = fieldInfoOriginal.getFields();
        CdmFieldEntry field1 = new CdmFieldEntry();
        field1.setNickName(ExportObjectsInfo.RECORD_ID);
        field1.setExportAs(ExportObjectsInfo.RECORD_ID);
        field1.setDescription(ExportObjectsInfo.RECORD_ID);
        field1.setSkipExport(false);
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName(ExportObjectsInfo.FILE_PATH);
        field2.setExportAs(ExportObjectsInfo.FILE_PATH);
        field2.setDescription(ExportObjectsInfo.FILE_PATH);
        field2.setSkipExport(false);
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        assertTrue(Files.size(project.getFieldsPath()) > 0, "Fields file must not be empty");

        CdmFieldInfo fieldInfoLoaded = service.loadFieldsFromProject(project);
        List<CdmFieldEntry> fieldsLoaded = fieldInfoLoaded.getFields();
        assertEquals(2, fieldsLoaded.size());
        assertHasFieldWithValue(ExportObjectsInfo.RECORD_ID, ExportObjectsInfo.RECORD_ID, ExportObjectsInfo.RECORD_ID,
                false, "", "", "",
                "", "", fieldsLoaded);
        assertHasFieldWithValue(ExportObjectsInfo.FILE_PATH, ExportObjectsInfo.FILE_PATH, ExportObjectsInfo.FILE_PATH,
                false, "", "", "",
                "", "", fieldsLoaded);
    }

    private void assertHasFieldWithValue(String nick, String expectedExport, String expectedDesc,
            boolean expectedSkip, String expectedCdmRequired, String expectedCdmSearchable, String expectedCdmHidden,
            String expectedCdmVocab, String expectedCdmDcMapping, List<CdmFieldEntry> fields) {
        Optional<CdmFieldEntry> matchOpt = fields.stream().filter(f -> nick.equals(f.getNickName())).findFirst();
        assertTrue(matchOpt.isPresent(), "Field " + nick + " not present");
        CdmFieldEntry entry = matchOpt.get();
        assertEquals(expectedDesc, entry.getDescription());
        assertEquals(expectedExport, entry.getExportAs());
        assertEquals(expectedSkip, entry.getSkipExport());
        assertEquals(expectedCdmRequired, entry.getCdmRequired());
        assertEquals(expectedCdmSearchable, entry.getCdmSearchable());
        assertEquals(expectedCdmHidden, entry.getCdmHidden());
        assertEquals(expectedCdmVocab, entry.getCdmVocab());
        assertEquals(expectedCdmDcMapping, entry.getCdmDcMapping());
    }
}
