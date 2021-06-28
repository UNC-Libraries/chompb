/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.unc.lib.boxc.migration.cdm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmFieldService service;
    @Mock
    private CloseableHttpClient httpClient;
    @Mock
    private CloseableHttpResponse httpResp;
    @Mock
    private HttpEntity respEntity;

    @Before
    public void setup() throws Exception {
        initMocks(this);
        tmpFolder.create();
        project = MigrationProjectFactory.createMigrationProject(tmpFolder.getRoot().toPath(), "proj", null, "user");
        service = new CdmFieldService();
        service.setHttpClient(httpClient);
        service.setCdmBaseUri(CDM_BASE_URL);

        when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResp);
        when(httpResp.getEntity()).thenReturn(respEntity);
    }

    @Test
    public void retrieveCdmFieldsTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/cdm_fields_resp.json"));

        CdmFieldInfo fieldInfo = service.retrieveFieldsForCollection(project);
        List<CdmFieldEntry> fields = fieldInfo.getFields();

        assertHasFieldWithValue("title", "title", "Title", false, fields);
        assertHasFieldWithValue("creato", "creato", "Creator", false, fields);
        assertHasFieldWithValue("date", "date", "Creation Date", false, fields);
        assertHasFieldWithValue("data", "data", "Date", false, fields);
        assertHasFieldWithValue("digitb", "digitb", "Digital Collection", false, fields);
    }

    @Test
    public void retrieveCdmFieldsInvalidResponseTest() throws Exception {
        when(respEntity.getContent()).thenReturn(new ByteArrayInputStream("well that's not right".getBytes()));

        try {
            service.retrieveFieldsForCollection(project);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Failed to parse response from URL"));
            assertTrue(e.getMessage().contains("Unrecognized token"));
        }
    }

    @Test
    public void retrieveCdmFieldsIncorrectStructureTest() throws Exception {
        when(respEntity.getContent()).thenReturn(new ByteArrayInputStream("{ 'oh' : 'no' }".getBytes()));

        try {
            service.retrieveFieldsForCollection(project);
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Unexpected response from URL"));
            assertTrue(e.getMessage().contains("It must be a JSON array"));
        }
    }

    @Test(expected = IOException.class)
    public void retrieveCdmFieldsConnectionFailureTest() throws Exception {
        when(httpClient.execute(any(HttpGet.class))).thenThrow(new IOException("Fail"));

        service.retrieveFieldsForCollection(project);
    }

    @Test
    public void persistEmptyFieldsTest() throws Exception {
        CdmFieldInfo fieldInfo = new CdmFieldInfo();
        service.persistFieldsToProject(project, fieldInfo);
        assertTrue("Fields file must not be empty", Files.size(project.getFieldsPath()) > 0);
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
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field2");
        field2.setExportAs("secondary");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        assertTrue("Fields file must not be empty", Files.size(project.getFieldsPath()) > 0);

        CdmFieldInfo fieldInfoLoaded = service.loadFieldsFromProject(project);
        List<CdmFieldEntry> fieldsLoaded = fieldInfoLoaded.getFields();
        assertEquals(2, fieldsLoaded.size());
        assertHasFieldWithValue("field1", "field1", "First field", false, fieldsLoaded);
        assertHasFieldWithValue("field2", "secondary", "Secondary Field", true, fieldsLoaded);
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
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field1");
        field2.setExportAs("secondary");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected error message " + e.getMessage(),
                    e.getMessage().contains("Duplicate cdm_nick value 'field1' at line 3"));
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
        fieldsOriginal.add(field1);

        CdmFieldEntry field2 = new CdmFieldEntry();
        field2.setNickName("field2");
        field2.setExportAs("field1");
        field2.setDescription("Secondary Field");
        field2.setSkipExport(true);
        fieldsOriginal.add(field2);

        service.persistFieldsToProject(project, fieldInfoOriginal);
        try {
            service.validateFieldsFile(project);
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected error message " + e.getMessage(),
                    e.getMessage().contains("Duplicate export_as value 'field1' at line 3"));
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
            assertTrue(e.getMessage().contains("Invalid CDM fields entry at line 3"));
        }
    }

    @Test(expected = NoSuchFileException.class)
    public void loadMissingFieldsFileTest() throws Exception {
        service.loadFieldsFromProject(project);
    }

    @Test
    public void retrieveValidateAndReloadRoundTripTest() throws Exception {
        when(respEntity.getContent()).thenReturn(this.getClass().getResourceAsStream("/cdm_fields_resp.json"));

        CdmFieldInfo fieldInfoRetrieved = service.retrieveFieldsForCollection(project);
        service.persistFieldsToProject(project, fieldInfoRetrieved);

        service.validateFieldsFile(project);

        CdmFieldInfo fieldInfoLoaded = service.loadFieldsFromProject(project);
        List<CdmFieldEntry> fieldsLoaded = fieldInfoLoaded.getFields();

        assertHasFieldWithValue("title", "title", "Title", false, fieldsLoaded);
        assertHasFieldWithValue("creato", "creato", "Creator", false, fieldsLoaded);
        assertHasFieldWithValue("date", "date", "Creation Date", false, fieldsLoaded);
        assertHasFieldWithValue("data", "data", "Date", false, fieldsLoaded);
        assertHasFieldWithValue("digitb", "digitb", "Digital Collection", false, fieldsLoaded);
    }

    private void assertHasFieldWithValue(String nick, String expectedExport, String expectedDesc,
            boolean expectedSkip, List<CdmFieldEntry> fields) {
        Optional<CdmFieldEntry> matchOpt = fields.stream().filter(f -> nick.equals(f.getNickName())).findFirst();
        assertTrue("Field " + nick + " not present", matchOpt.isPresent());
        CdmFieldEntry entry = matchOpt.get();
        assertEquals(expectedDesc, entry.getDescription());
        assertEquals(expectedExport, entry.getExportAs());
        assertEquals(expectedSkip, entry.getSkipExport());

    }
}
