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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import joptsimple.internal.Strings;

/**
 * @author bbpennel
 */
public class CdmIndexServiceTest {
    private static final String PROJECT_NAME = "proj";
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmIndexService service;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.getRoot().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        service = new CdmIndexService();
        service.setFieldService(fieldService);
        service.setProject(project);
    }

    @Test
    public void indexExportOneFileTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_all.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(3);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + Strings.join(exportFields, ",")
                    + " from " + CdmIndexService.TB_NAME + " order by cdmid asc");
            rs.next();
            assertEquals(25, rs.getInt("cdmid"));
            assertEquals("2005-11-23", rs.getString("cdmcreated"));
            assertEquals("Redoubt C", rs.getString("title"));
            assertEquals("Paper is discolored.", rs.getString("notes"));
            assertEquals("276_182_E.tif", rs.getString("file"));
            try {
                rs.getString("search");
                fail("Skipped field must not be indexed");
            } catch (SQLException e) {
            }

            rs.next();
            assertEquals(26, rs.getInt("cdmid"));
            assertEquals("2005-11-24", rs.getString("cdmcreated"));
            assertEquals("Plan of Battery McIntosh", rs.getString("title"));
            assertEquals("Paper", rs.getString("medium"));
            assertEquals("276_183B_E.tif", rs.getString("file"));

            rs.next();
            assertEquals(27, rs.getInt("cdmid"));
            assertEquals("2005-12-08", rs.getString("cdmcreated"));
            assertEquals("Fort DeRussy on Red River, Louisiana", rs.getString("title"));
            assertEquals("Bill Richards", rs.getString("creatb"));
            assertEquals("276_203_E.tif", rs.getString("file"));

        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexExportMultiFileTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_2.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(5);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + Strings.join(exportFields, ",")
                    + " from " + CdmIndexService.TB_NAME + " order by cdmid asc");
            rs.next();
            assertEquals(25, rs.getInt("cdmid"));
            assertEquals("Redoubt C", rs.getString("title"));

            rs.next();
            rs.next();

            rs.next();
            assertEquals(28, rs.getInt("cdmid"));
            assertEquals("2005-12-09", rs.getString("cdmcreated"));
            assertEquals("6337 x 9075", rs.getString("pixel"));
            assertEquals("276_241_E.tif", rs.getString("file"));

            rs.next();
            assertEquals(29, rs.getInt("cdmid"));
            assertEquals("2005-12-10", rs.getString("cdmcreated"));
            assertEquals("United States--History--Civil War, 1861-1865.;", rs.getString("subjea"));
            assertEquals("276_245a_E.tif", rs.getString("file"));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void exportIncompleteTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());

        service.createDatabase(false);
        try {
            service.indexAll();
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Must complete an export of the collection"));
            assertDateIndexedNotPresent();
        }
    }

    @Test
    public void indexAlreadyExistsTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        try {
            service.createDatabase(false);
            fail();
        } catch (StateAlreadyExistsException e) {
            assertTrue(e.getMessage().contains("Cannot create index, an index file already exists"));
            assertDateIndexedNotPresent();
        }
    }

    @Test
    public void indexAlreadyExistsForceFlagTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();
        assertRowCount(3);

        // Switch export files to one with fewer records and force a reindex
        Files.copy(Paths.get("src/test/resources/sample_exports/export_2.xml"),
                project.getExportPath().resolve("export_1.xml"), StandardCopyOption.REPLACE_EXISTING);
        service.createDatabase(true);
        service.indexAll();
        assertRowCount(2);

        assertDateIndexedPresent();
    }

    @Test
    public void removeIndexTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_1.xml"));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();
        assertRowCount(3);

        service.removeIndex();

        assertTrue(Files.notExists(project.getIndexPath()));
        assertDateIndexedNotPresent();
    }

    @Test
    public void invalidExportFileTest() throws Exception {
        FileUtils.write(project.getExportPath().resolve("export_1.xml").toFile(), "uh oh", ISO_8859_1);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        try {
            service.indexAll();
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Failed to parse export file"));
        }
    }

    @Test
    public void missingConfiguredFieldTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/sample_exports/export_1.xml"),
                project.getExportPath().resolve("export_all.xml"));
        String fieldString = FileUtils.readFileToString(new File("src/test/resources/gilmer_fields.csv"), ISO_8859_1);
        fieldString += "\nmystery,mystery,Mysterious,false";
        FileUtils.writeStringToFile(project.getFieldsPath().toFile(), fieldString, ISO_8859_1);
        setExportedDate();

        try {
            service.createDatabase(false);
            service.indexAll();
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Missing configured field mystery"));
        }
    }

    private void assertDateIndexedPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNotNull(props.getIndexedDate());
    }

    private void assertDateIndexedNotPresent() throws Exception {
        MigrationProjectProperties props = ProjectPropertiesSerialization.read(project.getProjectPropertiesPath());
        assertNull(props.getIndexedDate());
    }

    private void setExportedDate() throws Exception {
        project.getProjectProperties().setExportedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertRowCount(int expected) throws Exception {
        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            rs.next();
            assertEquals("Incorrect number of rows in database", expected, rs.getInt(1));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }
}
