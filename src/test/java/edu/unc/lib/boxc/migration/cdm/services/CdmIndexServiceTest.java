package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.exceptions.StateAlreadyExistsException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.io.FileUtils;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author bbpennel
 */
public class CdmIndexServiceTest {
    private static final String PROJECT_NAME = "proj";
    @TempDir
    public Path tmpFolder;

    private MigrationProject project;
    private CdmFieldService fieldService;
    private CdmIndexService service;

    @BeforeEach
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, "user", CdmEnvironmentHelper.DEFAULT_ENV_ID);
        Files.createDirectories(project.getExportPath());

        fieldService = new CdmFieldService();
        service = new CdmIndexService();
        service.setFieldService(fieldService);
        service.setProject(project);
    }

    @Test
    public void indexExportOneFileTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(161);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", exportFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc");
            rs.next();
            assertEquals(25, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2005-11-23", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("Redoubt C", rs.getString("title"));
            assertEquals("Paper is discolored.", rs.getString("notes"));
            assertEquals("276_182_E.tif", rs.getString("file"));
            try {
                rs.getString("search");
                fail("Skipped field must not be indexed");
            } catch (SQLException e) {
            }

            rs.next();
            assertEquals(26, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2005-11-24", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("Plan of Battery McIntosh", rs.getString("title"));
            assertEquals("Paper", rs.getString("medium"));
            assertEquals("276_183_E.tif", rs.getString("file"));

            rs.next();
            assertEquals(27, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2005-12-08", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("Fort DeRussy on Red River, Louisiana", rs.getString("title"));
            assertEquals("Bill Richards", rs.getString("creatb"));
            assertEquals("276_203_E.tif", rs.getString("file"));

        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexAlreadyExistsTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
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
        Files.copy(Paths.get("src/test/resources/descriptions/mini_gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();
        assertRowCount(3);

        // Switch desc to full set and force a reindex
        Files.copy(Paths.get("src/test/resources/descriptions/gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project), StandardCopyOption.REPLACE_EXISTING);
        service.createDatabase(true);
        service.indexAll();
        assertRowCount(161);

        assertDateIndexedPresent();
    }

    @Test
    public void removeIndexTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/mini_gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
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
        FileUtils.write(CdmFileRetrievalService.getDescAllPath(project).toFile(), "uh oh", ISO_8859_1);
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        try {
            service.indexAll();
            fail();
        } catch (MigrationException e) {
            assertTrue(e.getMessage().contains("Failed to parse desc.all file"));
        }
    }

    @Test
    public void missingConfiguredFieldTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/mini_gilmer/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        String fieldString = FileUtils.readFileToString(new File("src/test/resources/gilmer_fields.csv"), ISO_8859_1);
        fieldString += "\nmystery,mystery,Mysterious,false,0,0,0,0,mystery";
        FileUtils.writeStringToFile(project.getFieldsPath().toFile(), fieldString, ISO_8859_1);
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", exportFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc limit 1");
            rs.next();
            assertEquals(25, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Redoubt C", rs.getString("title"));
            assertEquals("", rs.getString("mystery"));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexExportWithCompoundObjectsTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.createDirectories(CdmFileRetrievalService.getExportedCpdsPath(project));
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/image/617.cpd"),
                CdmFileRetrievalService.getExportedCpdsPath(project).resolve("617.cpd"));
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/image/620.cpd"),
                CdmFileRetrievalService.getExportedCpdsPath(project).resolve("620.cpd"));
        Files.copy(Paths.get("src/test/resources/keepsakes_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(7);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> allFields = fieldInfo.listAllExportFields();
        allFields.addAll(CdmIndexService.MIGRATION_FIELDS);

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", allFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc");
            rs.next();
            assertEquals(216, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2012-05-18", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2014-01-17", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Playmakers, circa 1974", rs.getString("title"));
            assertNull(rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(602, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("World War II ration book", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("604", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(603, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("World War II ration book (instructions)", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("604", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(604, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2014-01-17", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2014-01-17", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("World War II ration book, 1943", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(605, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("607", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(606, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2012-09-11", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial (open, next to box)", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("607", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(607, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2014-02-17", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2014-02-17", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexExportWithMissingCompoundObjectTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.createDirectories(CdmFileRetrievalService.getExportedCpdsPath(project));
        Files.copy(Paths.get("src/test/resources/descriptions/mini_keepsakes/image/620.cpd"),
                CdmFileRetrievalService.getExportedCpdsPath(project).resolve("620.cpd"));
        Files.copy(Paths.get("src/test/resources/keepsakes_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(7);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> allFields = fieldInfo.listAllExportFields();
        allFields.addAll(CdmIndexService.MIGRATION_FIELDS);

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", allFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc");
            rs.next();
            assertEquals(216, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Playmakers, circa 1974", rs.getString("title"));

            rs.next();
            assertEquals(602, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("World War II ration book", rs.getString("title"));
            // Without the CPD file, there is no way to know that 602 and 603 are children, so they become standalone
            assertNull(rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(603, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("World War II ration book (instructions)", rs.getString("title"));
            assertNull(rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(604, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("World War II ration book, 1943", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(605, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial (closed, in box)", rs.getString("title"));
            assertEquals("607", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(606, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial (open, next to box)", rs.getString("title"));
            assertEquals("607", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(607, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Tiffany's pillbox commemorating UNC's bicentennial", rs.getString("title"));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
        var warnings = service.getIndexingWarnings();
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("not found"));
        assertTrue(warnings.get(0).contains("object 604"));
        assertTrue(warnings.get(0).contains("617.cpd"));
    }

    @Test
    public void indexExportWithMonographCompoundObjectsTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/monograph/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.createDirectories(CdmFileRetrievalService.getExportedCpdsPath(project));
        Files.copy(Paths.get("src/test/resources/descriptions/monograph/image/196.cpd"),
                CdmFileRetrievalService.getExportedCpdsPath(project).resolve("196.cpd"));
        Files.copy(Paths.get("src/test/resources/monograph_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(4);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> allFields = fieldInfo.listAllExportFields();
        allFields.addAll(CdmIndexService.MIGRATION_FIELDS);

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + String.join(",", allFields)
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc");
            rs.next();
            assertEquals(192, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2007-12-13", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2008-12-16", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Page 1", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("195", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(193, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2007-12-13", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2008-12-18", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Page 2", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("195", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(194, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2007-12-13", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2008-12-16", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Page 3", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertEquals("195", rs.getString(CdmIndexService.PARENT_ID_FIELD));

            rs.next();
            assertEquals(195, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("2020-12-10", rs.getString(CdmFieldInfo.CDM_CREATED));
            assertEquals("2020-12-10", rs.getString(CdmFieldInfo.CDM_MODIFIED));
            assertEquals("Map of the Surveys for Atlantic and N. Carolina R-Road", rs.getString("title"));
            assertEquals(CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT, rs.getString(CdmIndexService.ENTRY_TYPE_FIELD));
            assertNull(rs.getString(CdmIndexService.PARENT_ID_FIELD));
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void indexExportReservedWordFieldTest() throws Exception {
        Files.copy(Paths.get("src/test/resources/descriptions/03883/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project));
        Files.copy(Paths.get("src/test/resources/roy_brown/cdm_fields.csv"), project.getFieldsPath());
        setExportedDate();

        service.createDatabase(false);
        service.indexAll();

        assertDateIndexedPresent();
        assertRowCount(2);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        List<String> exportFields = fieldInfo.listAllExportFields();

        Connection conn = service.openDbConnection();
        try {
            Statement stmt = conn.createStatement();
            String fields = exportFields.stream().map(f -> '"' + f + '"').collect(Collectors.joining(","));
            ResultSet rs = stmt.executeQuery("select " + fields
                    + " from " + CdmIndexService.TB_NAME + " order by " + CdmFieldInfo.CDM_ID + " asc");
            rs.next();
            assertEquals(0, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Folder 5: 1950-1956: Scan 25", rs.getString("title"));

            rs.next();
            assertEquals(548, rs.getInt(CdmFieldInfo.CDM_ID));
            assertEquals("Folder 9: Reports and writings on social work: Scan 51", rs.getString("title"));

        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }

    @Test
    public void buildDocumentNormalTest() throws Exception {
        var body = "<subjec>Maps</subjec>\n" +
                   "<titla>Test\n\nTitle</titla>\n" +
                   "<dmrecord>0</dmrecord>\n";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals("Maps", rootEl.getChildText("subjec"));
        assertEquals("Test\n\nTitle", rootEl.getChildText("titla"));
        assertEquals("0", rootEl.getChildText("dmrecord"));
    }

    @Test
    public void buildDocumentGreaterLessThanInContentTest() throws Exception {
        var body = "<subjec>Maps></subjec>\n" +
                   "<titla>Test < Title</titla>\n" +
                   "<dmrecord>0</dmrecord>\n";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals("Maps>", rootEl.getChildText("subjec"));
        assertEquals("Test < Title", rootEl.getChildText("titla"));
    }

    @Test
    public void buildDocumentAmpersandInContentTest() throws Exception {
        var body = "<subjec>M&ps</subjec>\n" +
                   "<titla>Test & Title</titla>\n" +
                   "<dmrecord>0</dmrecord>\n";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals("M&ps", rootEl.getChildText("subjec"));
        assertEquals("Test & Title", rootEl.getChildText("titla"));
    }

    @Test
    public void buildDocumentUnmatchedClosingTagTest() throws Exception {
        var body = "<subjec>Maps</subjec>\n" +
                "<titla>Test Weird Closing Title</transc>\n" +
                "<dmrecord>0</dmrecord>\n";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals("Maps", rootEl.getChildText("subjec"));
        assertEquals("Test Weird Closing Title", rootEl.getChildText("titla"));
        assertEquals("0", rootEl.getChildText("dmrecord"));
    }

    @Test
    public void buildDocumentInvalidUnicodeTest() throws Exception {
        var body = "<subjec>Maps</subjec>\n" +
                   "<titla>Test " + Character.toString(0xb) + " Title</titla>\n" +
                   "<dmrecord>0</dmrecord>\n";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals("Maps", rootEl.getChildText("subjec"));
        assertEquals("Test  Title", rootEl.getChildText("titla"));
        assertEquals("0", rootEl.getChildText("dmrecord"));
    }

    @Test
    public void buildDocumentWithLongFieldTest() throws Exception {
        var body = "<title>Fencing with Fidel</title>\n" +
                "<subjec></subjec>\n" +
                "<descri></descri>\n" +
                "<creato></creato>\n" +
                "<publis></publis>\n" +
                "<contri></contri>\n" +
                "<date></date>\n" +
                "<type></type>\n" +
                "<format></format>\n" +
                "<identi></identi>\n" +
                "<source></source>\n" +
                "<langua></langua>\n" +
                "<relati></relati>\n" +
                "<covera></covera>\n" +
                "<rights></rights>\n" +
                "<audien></audien>\n" +
                "<full>page 6\n" +
                "which is the traditional diplomatic memoir, does not give the reader the flavor of life in\n" +
                "the Foreign Service. For me, it was the more peripheral experiences and the human\n" +
                "interaction of policy development and implementation that provided flavor.\n" +
                "I was in the Foreign Service for 31 years. ttwas my entire adult life until I retired. I\n" +
                "married immediately after graduation from Princeton and was in the Service that\n" +
                "September. A1f but five of those years were spent abroad, always in Latin America.\n" +
                "I served from Argentina to Mexico, in the Andes, \" throughout Central America and\n" +
                "twice in the Caribbean. Although after retirement 1 continued working in the foreign\n" +
                "affairs field at the CIA and at the Department of Labor, I have always considered the\n" +
                "Foreign Service to be my career.\n" +
                "This is not a retelling of my career and of the times in which it was rooted: Rather, it Is\n" +
                "a ' selective1 memoir because it omits much of the actual work I did as an economist\n" +
                "and executive. And I do not directly discuss the policies that stiaped U. S. relations\n" +
                "with the countries in which I served. What I have selected for inclusion are those\n" +
                "experiences that strike me as providing insights into the peoples and eultures of Latin\n" +
                "America, and into the personalities of diplomats themselves, including myself,\n" +
                "insights as perceived initially by a very callow young American who, one hopes,\n" +
                "matured as thedecades passed. I tiave also selected many incidents in which my\n" +
                "wife, Sue, was involved. For us, the Foreign Service definitely was a Twofer4\n" +
                "arrangement; the Department of State got two for the price of one wttenihey hired\n" +
                "me and I married Sue. Sue and I shared a career.\n" +
                "All of the situations I describe in this memoir, ™ matter how strange and foreign,\n" +
                "happened to me. IVe tried to describe them accurately. But I admit to some literary^M<</full>\n" +
                "<fullrs></fullrs>\n" +
                "<find>9.pdfpage</find>\n" +
                "<dmaccess></dmaccess>\n" +
                "<dmoclcno></dmoclcno>\n" +
                "<dmcreated>2009-07-28</dmcreated>\n" +
                "<dmmodified>2009-07-28</dmmodified>\n" +
                "<dmrecord>7</dmrecord>";
        var rootEl = service.buildDocument(body).getRootElement();
        assertEquals("record", rootEl.getName());
        assertEquals(1844, rootEl.getChildText("full").length());
        assertEquals("Fencing with Fidel", rootEl.getChildText("title"));
        assertEquals("7", rootEl.getChildText("dmrecord"));
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
            assertEquals(expected, rs.getInt(1), "Incorrect number of rows in database");
        } finally {
            CdmIndexService.closeDbConnection(conn);
        }
    }
}
