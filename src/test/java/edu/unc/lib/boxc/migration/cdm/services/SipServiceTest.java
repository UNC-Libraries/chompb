package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.AltTextInfo;
import edu.unc.lib.boxc.migration.cdm.model.AspaceRefIdInfo;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.AggregateFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingSyncOptions;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.test.BxcEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.CdmEnvironmentHelper;
import edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrAcl;
import edu.unc.lib.boxc.model.api.rdf.CdrAspace;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil;
import edu.unc.lib.boxc.model.fcrepo.ids.PIDs;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.jdom2.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static edu.unc.lib.boxc.migration.cdm.services.sips.WorkGenerator.STREAMING_TYPE;
import static edu.unc.lib.boxc.migration.cdm.services.sips.WorkGenerator.STREAMING_URL;
import static edu.unc.lib.boxc.migration.cdm.test.PostMigrationReportTestHelper.assertContainsRow;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

/**
 * @author bbpennel
 */
public class SipServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    private static final String DEST_UUID = "bfe93126-849a-43a5-b9d9-391e18ffacc6";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";
    private static final String DEST_UUID3 = "bdbd99af-36a5-4bab-9785-e3a802d3737e";
    private static final String SOLR_URL = "http://example.com:88/solr";

    @TempDir
    public Path tmpFolder;

    @Mock
    private HttpSolrClient solrClient;

    @Captor
    private ArgumentCaptor<SolrQuery> solrQueryCaptor;

    private MigrationProject project;
    private SipService service;
    private SipServiceHelper testHelper;
    private AutoCloseable closeable;

    @BeforeEach
    public void setup() throws Exception {
        closeable = openMocks(this);
        setupProject(CdmEnvironmentHelper.DEFAULT_ENV_ID, MigrationProject.PROJECT_SOURCE_CDM);
    }

    private void setupProject(String cdmEnvId, String projectSource) throws Exception {
        FileUtils.deleteQuietly(tmpFolder.resolve(PROJECT_NAME).toFile());

        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder, PROJECT_NAME, null, USERNAME, cdmEnvId,
                BxcEnvironmentHelper.DEFAULT_ENV_ID, projectSource);

        testHelper = new SipServiceHelper(project, tmpFolder);
        service = testHelper.createSipsService();
        testHelper.getArchivalDestinationsService().setSolr(solrClient);
        testHelper.getArchivalDestinationsService().setSolrServerUrl(SOLR_URL);
    }

    @AfterEach
    void closeService() throws Exception {
        closeable.close();
    }

    @Test
    public void generateSipsDataNotIndexed() throws Exception {
        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Exported data must be indexed"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsDestinationsNotGenerated() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        testHelper.populateDescriptions("gilmer_mods1.xml");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Destinations must be mapped"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsDescriptionsNotGenerated() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Descriptions must be created"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsSourceFilesNotMapped() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("Cannot transform object 25, no source file has been mapped"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsSingleDestination() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsNewCollectionDestination() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, "001234");
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(1, depBagChildren.size());

        Resource collResc = depBagChildren.iterator().next().asResource();
        assertTrue(collResc.hasProperty(RDF.type, Cdr.Collection));
        assertTrue(collResc.hasProperty(CdrDeposit.label, "001234"));
        assertTrue(collResc.hasProperty(CdrAcl.none, PUBLIC_PRINC));
        assertTrue(collResc.hasProperty(CdrAcl.none, AUTHENTICATED_PRINC));
        testHelper.assertMigrationEventPresent(dirManager, sip.getNewCollectionPid());
        Bag collBag = model.getBag(collResc);
        List<RDFNode> collChildren = collBag.iterator().toList();

        Resource workResc1 = testHelper.getResourceByCreateTime(collChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(collChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(collChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        assertEquals(3, collChildren.size());

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsNewCollectionDestinationWithDescription() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        String newCollId = "00123a";
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, newCollId);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        // Add a mods file at the expected path
        Path newCollDescPath = project.getNewCollectionDescriptionsPath().resolve(newCollId + ".xml");
        String newCollDesc = "<mods:mods xmlns:mods=\"http://www.loc.gov/mods/v3\"><mods:titleInfo>"
                + "<mods:title>New Collection</mods:title></mods:titleInfo></mods:mods>";
        FileUtils.write(newCollDescPath.toFile(), newCollDesc, StandardCharsets.UTF_8);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(1, depBagChildren.size());

        Resource collResc = depBagChildren.iterator().next().asResource();
        assertTrue(collResc.hasProperty(RDF.type, Cdr.Collection));
        assertTrue(collResc.hasProperty(CdrDeposit.label, newCollId));
        assertTrue(collResc.hasProperty(CdrAcl.none, PUBLIC_PRINC));
        assertTrue(collResc.hasProperty(CdrAcl.none, AUTHENTICATED_PRINC));
        testHelper.assertMigrationEventPresent(dirManager, sip.getNewCollectionPid());
        // Check that desc file was copied over
        Path path = dirManager.getModsPath(sip.getNewCollectionPid());
        Document doc = SecureXMLFactory.createSAXBuilder().build(path.toFile());
        String title = doc.getRootElement().getChild("titleInfo", JDOMNamespaceUtil.MODS_V3_NS)
                .getChildText("title", JDOMNamespaceUtil.MODS_V3_NS);
        assertEquals("New Collection", title);

        Bag collBag = model.getBag(collResc);
        List<RDFNode> collChildren = collBag.iterator().toList();
        assertEquals(3, collChildren.size());

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithAccessFiles() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        List<Path> accessLocs = testHelper.populateAccessFiles("276_182_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), accessLocs.get(0), "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), accessLocs.get(1), "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsMissingSourceFile() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        // Only populating 2 out of 3 source files expected from the export
        testHelper.populateSourceFiles("276_203_E.tif");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("no source file has been mapped"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsMissingSourceFileWithForce() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        // Only populating 2 out of 3 source files expected from the export
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_203_E.tif");

        SipGenerationOptions options = makeOptions(true);

        List<MigrationSip> sips = service.generateSips(options);
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(1), null, "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsMissingDescription() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        // Deleting one description so that it is missing
        Files.delete(testHelper.getDescriptionsService().getExpandedDescriptionFilePath("26"));
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue(e.getMessage().contains("does not have a MODS description"),
                    "Unexpected message: " + e.getMessage());
        }
    }

    @Test
    public void generateSipsMissingDescriptionWithForce() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        // Deleting one description so that it is missing
        Files.delete(testHelper.getDescriptionsService().getExpandedDescriptionFilePath("27"));
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        SipGenerationOptions options = makeOptions(true);

        List<MigrationSip> sips = service.generateSips(options);
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsMultipleDestinations() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID2 + ",");
        }

        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(2, sips.size());
        MigrationSip sip1 = sips.get(0);

        assertTrue(Files.exists(sip1.getSipPath()));
        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip1);

        Model model = testHelper.getSipModel(sip1);

        Bag depBag = model.getBag(sip1.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        assertPersistedSipInfoMatches(sip1);

        MigrationSip sip2 = sips.get(1);

        assertTrue(Files.exists(sip2.getSipPath()));
        DepositDirectoryManager dirManager2 = testHelper.createDepositDirectoryManager(sip2);

        Model model2 = testHelper.getSipModel(sip2);

        Bag depBag2 = model2.getBag(sip2.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren2 = depBag2.iterator().toList();
        assertEquals(1, depBagChildren2.size());

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren2, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager2, model2, stagingLocs.get(1), null, "26");

        assertPersistedSipInfoMatches(sip2);

        List<MigrationSip> listedSips = service.listSips();
        assertEquals(2, listedSips.size());
    }

    @Test
    public void generateSipsWithGroupedWork() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif");
        setupGroupIndex();

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        // Assert that children of grouped work have descriptions added (only second file as a description)
        Resource work1File1Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(0));
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work1File2Resc.getURI()), "26");
        // Second file should be ordered before the first file for the grouped work
        String work1Members = PIDs.get(work1File2Resc.getURI()).getId() + "|" + PIDs.get(work1File1Resc.getURI()).getId();
        assertTrue(workResc1.hasProperty(Cdr.memberOrder, work1Members));
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertFalse(workResc2.hasProperty(Cdr.memberOrder), "Work with group field but only one file should not have order");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertFalse(workResc3.hasProperty(Cdr.memberOrder), "Regular work should not have order");
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertFalse(workResc4.hasProperty(Cdr.memberOrder), "Regular work should not have order");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithGroupedWorkMultipleDestinations() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("grp:groupa:group1," + DEST_UUID2 + ",");
        }
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif");
        setupGroupIndex();

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(2, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertFalse(workResc2.hasProperty(Cdr.memberOrder), "Work with group field but only one file should not have order");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertFalse(workResc3.hasProperty(Cdr.memberOrder), "Regular work should not have order");
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertFalse(workResc4.hasProperty(Cdr.memberOrder), "Regular work should not have order");

        assertPersistedSipInfoMatches(sip);

        MigrationSip sip2 = sips.get(1);

        assertTrue(Files.exists(sip2.getSipPath()));

        DepositDirectoryManager dirManager2 = testHelper.createDepositDirectoryManager(sip2);

        Model model2 = testHelper.getSipModel(sip2);
        Bag depBag2 = model2.getBag(sip2.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren2 = depBag2.iterator().toList();
        assertEquals(1, depBagChildren2.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren2, "2005-11-23");
        Bag work1Bag = model2.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager2, model2, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        // Assert that children of grouped work have descriptions added (only second file as a description)
        Resource work1File1Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(0));
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        testHelper.assertModsPresentWithCdmId(dirManager2, PIDs.get(work1File2Resc.getURI()), "26");
        // Second file should be ordered before the first file for the grouped work
        String work1Members = PIDs.get(work1File2Resc.getURI()).getId() + "|" + PIDs.get(work1File1Resc.getURI()).getId();
        assertTrue(workResc1.hasProperty(Cdr.memberOrder, work1Members));
    }

    @Test
    public void generateSipsWithGroupedWorkWithAggregateFiles() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif", "group1.pdf", "group1_top.pdf", "group1_bottom.pdf", "group1_2bottom.pdf");
        setupGroupIndex();

        // Adding first file at top of work
        var aggregateTopService = testHelper.getAggregateFileMappingService();
        var options = new AggregateFileMappingOptions();
        options.setBasePath(testHelper.getSourceFilesBasePath());
        options.setExportField("groupa");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1.pdf");
        aggregateTopService.generateMapping(options);
        // Add second file at top
        options.setUpdate(true);
        options.setFilenameTemplate("$1_top.pdf");
        aggregateTopService.generateMapping(options);
        // Adding first file at bottom of work
        var aggregateBottomService = testHelper.getAggregateBottomMappingService();
        options.setFilenameTemplate("$1_bottom.pdf");
        aggregateBottomService.generateMapping(options);
        // Adding second file at bottom
        options.setFilenameTemplate("$1_2bottom.pdf");
        aggregateBottomService.generateMapping(options);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1), stagingLocs.get(5), stagingLocs.get(6), stagingLocs.get(7), stagingLocs.get(8));
        // Aggregates at top and bottom in order added, and second file before first
        var work1OrderList = Arrays.asList(findFileIdByStagingLocation(work1Bag, stagingLocs.get(5)),
                findFileIdByStagingLocation(work1Bag, stagingLocs.get(6)),
                findFileIdByStagingLocation(work1Bag, stagingLocs.get(1)),
                findFileIdByStagingLocation(work1Bag, stagingLocs.get(0)),
                findFileIdByStagingLocation(work1Bag, stagingLocs.get(7)),
                findFileIdByStagingLocation(work1Bag, stagingLocs.get(8)));
        var work1Members = String.join("|", work1OrderList);
        assertTrue(workResc1.hasProperty(Cdr.memberOrder, work1Members));

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertFalse(workResc2.hasProperty(Cdr.memberOrder), "Work with group field but only one file should not have order");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertFalse(workResc3.hasProperty(Cdr.memberOrder), "Regular work should not have order");
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertFalse(workResc4.hasProperty(Cdr.memberOrder), "Regular work should not have order");

        assertPersistedSipInfoMatches(sip);
    }

    private String findFileIdByStagingLocation(Bag workBag, Path stagingLoc) {
        Resource fileResc = testHelper.findChildByStagingLocation(workBag, stagingLoc);
        return PIDs.get(fileResc.getURI()).getId();
    }

    @Test
    public void generateSipsWithGroupedWorkWithAccessCopies() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        List<Path> accessLocs = testHelper.populateAccessFiles("276_182_E.tif", "276_203_E.tif");
        setupGroupIndex();

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", true,
                stagingLocs.get(0), accessLocs.get(0), stagingLocs.get(1), null);
        // Assert that children of grouped work have descriptions added (only second file has a description)
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work1File2Resc.getURI()), "26");

        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model,
                stagingLocs.get(2), accessLocs.get(1), "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithCompoundObjects() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        setupDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        List<Path> stagingLocs = testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2012-05-18");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "216");
        assertFalse(workResc1.hasProperty(Cdr.memberOrder), "Single file work should not have order");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2014-01-17");
        Bag work2Bag = model.getBag(workResc2);
        testHelper.assertGroupedWorkPopulatedInSip(workResc2, dirManager, model, "604", false,
                stagingLocs.get(1), stagingLocs.get(2));
        // Verify that the children of the compound object have descriptions added
        Resource work2File1Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(1));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work2File1Resc.getURI()), "602");
        Resource work2File2Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(2));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work2File2Resc.getURI()), "603");
        String work2Members = PIDs.get(work2File1Resc.getURI()).getId() + "|" + PIDs.get(work2File2Resc.getURI()).getId();
        assertTrue(workResc2.hasProperty(Cdr.memberOrder, work2Members));

        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2014-02-17");
        Bag work3Bag = model.getBag(workResc3);
        testHelper.assertGroupedWorkPopulatedInSip(workResc3, dirManager, model, "607", false,
                stagingLocs.get(3), stagingLocs.get(4));
        Resource work3File1Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(3));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work3File1Resc.getURI()), "605");
        Resource work3File2Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(4));
        testHelper.assertModsPresentWithCdmId(dirManager, PIDs.get(work3File2Resc.getURI()), "606");
        assertPersistedSipInfoMatches(sip);

        // CPD file specifies 606 should be before 605
        String work3Members = PIDs.get(work3File2Resc.getURI()).getId() + "|" + PIDs.get(work3File1Resc.getURI()).getId();
        assertTrue(workResc3.hasProperty(Cdr.memberOrder, work3Members));
    }

    @Test
    public void generateSipsWithRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        service.generateSips(makeOptions());

        assertTrue(Files.exists(project.getRedirectMappingPath()));

        try (
            Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withHeader(RedirectMappingService.CSV_HEADERS)
                    .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "26");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "27");
        }
    }

    @Test
    public void generateSipsWithMultipleDestinationsAndRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID2 + ",");
        }

        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(2, sips.size());

        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals(3, rows.toArray().length);
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "26");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "27");
            // there should not be a collection row
        }
    }

    @Test
    public void generateSipsWithGroupedWorkAndRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> paths = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        setupGroupIndex();

        service.generateSips(makeOptions());

        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "27"); // ungrouped items first
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "26");
            // Work generated for group should not have a redirect mapping
            assertRedirectMappingCollectionRowContentIsCorrect(rows.get(3), project, DEST_UUID);
            // grouped files should have the same boxc object ID
            assertEquals(rows.get(1).get("boxc_object_id"), rows.get(2).get("boxc_object_id"));
            assertEquals(4, rows.size());
        }
        var pmRows = PostMigrationReportTestHelper.parseReport(project);
        assertContainsRow(pmRows, "grp:groupa:group1",
                "",
                "Work",
                "Folder Group 1",
                "",
                "",
                "",
                "",
                "2");
        assertContainsRow(pmRows, "25",
                "http://localhost/cdm/singleitem/collection/proj/id/25",
                "File",
                "",
                "276_182_E.tif",
                paths.get(0).toString(),
                "",
                "Folder Group 1",
                "");
        assertContainsRow(pmRows, "26",
                "http://localhost/cdm/singleitem/collection/proj/id/26",
                "File",
                "Plan of Battery McIntosh",
                "276_183_E.tif",
                paths.get(1).toString(),
                "",
                "Folder Group 1",
                "");
        assertContainsRow(pmRows, "27",
                "http://localhost/cdm/singleitem/collection/proj/id/27",
                "Work",
                "Fort DeRussy on Red River, Louisiana",
                "276_203_E.tif",
                paths.get(2).toString(),
                "",
                "",
                "1");
        assertContainsRow(pmRows, "27/original_file",
                "http://localhost/cdm/singleitem/collection/proj/id/27",
                "File",
                "",
                "276_203_E.tif",
                paths.get(2).toString(),
                "",
                "Fort DeRussy on Red River, Louisiana",
                "");
    }

    @Test
    public void generateSipsTwiceOverwritesRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        service.generateSips(makeOptions());
        String redirectFile = FileUtils.readFileToString(project.getRedirectMappingPath().toFile(), StandardCharsets.UTF_8);

        service = testHelper.createSipsService();
        service.generateSips(makeOptions());
        String secondRedirectFile = FileUtils.readFileToString(project.getRedirectMappingPath().toFile(), StandardCharsets.UTF_8);


        assertTrue(Files.exists(project.getRedirectMappingPath()));
        // make sure that sequential sips generation object/file ID changes are reflected in the redirect mapping csv
        assertNotEquals(redirectFile, secondRedirectFile);
    }

    @Test
    public void generateSipWithCompoundObjectsAndRedirectMapping() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        setupDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        List<Path> stagingLocs = testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");

        service.generateSips(makeOptions());

        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "216");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "602");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "603");
            assertRedirectMappingRowContentNoFileId(rows.get(3), project, "604");
            assertRedirectMappingRowContentIsCorrect(rows.get(4), project, "606");
            assertRedirectMappingRowContentIsCorrect(rows.get(5), project, "605");
            assertRedirectMappingRowContentNoFileId(rows.get(6), project, "607");
            // collection row should redirect to the boxc destination ID
            assertRedirectMappingCollectionRowContentIsCorrect(rows.get(7), project, DEST_UUID);
            String cmpId1 = rows.get(3).get("boxc_object_id");
            assertEquals(cmpId1, rows.get(1).get("boxc_object_id"));
            assertEquals(cmpId1, rows.get(2).get("boxc_object_id"));
            String cmpId2 = rows.get(6).get("boxc_object_id");
            assertEquals(cmpId2, rows.get(4).get("boxc_object_id"));
            assertEquals(cmpId2, rows.get(5).get("boxc_object_id"));

            assertEquals(8, rows.size());
        }
    }

    @Test
    public void generateSipsWithMultipleNewCollectionsAndRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, "001234");
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID + ",005678");
        }

        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(2, sips.size());

        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals(4, rows.toArray().length);
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "26");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "27");
            // collection row should redirect to the boxc destination ID
            assertRedirectMappingCollectionRowContentIsCorrect(rows.get(3), project, DEST_UUID);
        }
    }

    @Test
    public void generateSipWithNewCollectionAndRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, "001234");
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        String boxcCollectionId = sips.get(0).getNewCollectionId();
        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            assertEquals(4, rows.toArray().length);
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "26");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "27");
            // collection row should redirect to the boxc collection ID
            assertRedirectMappingCollectionRowContentIsCorrect(rows.get(3), project, boxcCollectionId);
        }
    }

    @Test
    public void generateSipWithSuppressedCollectionRedirectMapping() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, "001234");
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        SipGenerationOptions options = new SipGenerationOptions();
        options.setUsername(USERNAME);
        options.setForce(false);
        options.setSuppressCollectionRedirect(true);

        service.generateSips(options);
        try (
                Reader reader = Files.newBufferedReader(project.getRedirectMappingPath());
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withHeader(RedirectMappingService.CSV_HEADERS)
                        .withTrim());
        ) {
            List<CSVRecord> rows = csvParser.getRecords();
            // collection row should not redirect
            assertEquals(3, rows.toArray().length);
            assertRedirectMappingRowContentIsCorrect(rows.get(0), project, "25");
            assertRedirectMappingRowContentIsCorrect(rows.get(1), project, "26");
            assertRedirectMappingRowContentIsCorrect(rows.get(2), project, "27");
        }
    }

    @Test
    public void generateSipsWithArchivalCollection() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        solrResponseWithPid();
        testHelper.generateArchivalCollectionDestinationMapping(DEST_UUID3, null, "groupa");
        testHelper.populateDescriptions("gilmer_mods1.xml", "gilmer_mods2.xml");
        testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif",
                "276_203_E.tif", "276_241_E.tif", "276_245a_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(3, sips.size());

        // groupa:group2 and groupa:group11
        MigrationSip sip1 = sips.get(0);
        assertTrue(Files.exists(sip1.getSipPath()));
        assertEquals(DEST_UUID2, sip1.getDestinationPid().getUUID());
        Model model = testHelper.getSipModel(sip1);
        Bag depBag = model.getBag(sip1.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());
        assertPersistedSipInfoMatches(sip1);

        // groupa:group1
        MigrationSip sip2 = sips.get(1);
        assertTrue(Files.exists(sip2.getSipPath()));
        assertEquals(DEST_UUID, sip2.getDestinationPid().getUUID());
        Model model2 = testHelper.getSipModel(sip2);
        Bag depBag2 = model2.getBag(sip2.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren2 = depBag2.iterator().toList();
        assertEquals(2, depBagChildren2.size());
        assertPersistedSipInfoMatches(sip2);

        // default
        MigrationSip sip3 = sips.get(2);
        assertTrue(Files.exists(sip3.getSipPath()));
        assertEquals(DEST_UUID3, sip3.getDestinationPid().getUUID());
        Model model3 = testHelper.getSipModel(sip3);
        Bag depBag3 = model3.getBag(sip3.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren3 = depBag3.iterator().toList();
        assertEquals(1, depBagChildren3.size());
        assertPersistedSipInfoMatches(sip3);
    }

    @Test
    public void generateSipsWithDefaultPermissions() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.generateDefaultPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        assertHasPermission(workResc1, CdrAcl.canViewMetadata);
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);

        Resource work1FileResc = getOnlyChildOf(workResc1);
        assertPermissionsUnassigned(work1FileResc);
        Resource work2FileResc = getOnlyChildOf(workResc2);
        assertPermissionsUnassigned(work2FileResc);
        Resource work3FileResc = getOnlyChildOf(workResc3);
        assertPermissionsUnassigned(work3FileResc);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithFilePermissions() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.generateFilePermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        assertDoesNotHavePermission(workResc1, CdrAcl.canViewMetadata);
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        assertDoesNotHavePermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        assertDoesNotHavePermission(workResc3, CdrAcl.canViewMetadata);

        Resource work1FileResc = getOnlyChildOf(workResc1);
        assertPermissionsUnassigned(work1FileResc);
        Resource work2FileResc = getOnlyChildOf(workResc2);
        assertPermissionsUnassigned(work2FileResc);
        Resource work3FileResc = getOnlyChildOf(workResc3);
        assertPermissionsUnassigned(work3FileResc);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithWorkPermissions() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        testHelper.generateWorkPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        assertHasPermission(workResc1, CdrAcl.canViewMetadata);
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);

        Resource work1FileResc = getOnlyChildOf(workResc1);
        assertPermissionsUnassigned(work1FileResc);
        Resource work2FileResc = getOnlyChildOf(workResc2);
        assertPermissionsUnassigned(work2FileResc);
        Resource work3FileResc = getOnlyChildOf(workResc3);
        assertPermissionsUnassigned(work3FileResc);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithCompoundObjectsPermissions() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.generateAllPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        setupDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        List<Path> stagingLocs = testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2012-05-18");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "216");
        assertHasPermission(workResc1, CdrAcl.canViewMetadata);
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2014-01-17");
        Bag work2Bag = model.getBag(workResc2);
        testHelper.assertGroupedWorkPopulatedInSip(workResc2, dirManager, model, "604", false,
                stagingLocs.get(1), stagingLocs.get(2));
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource work2File1Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(1));
        assertHasPermission(work2File1Resc, CdrAcl.canViewMetadata);
        Resource work2File2Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(2));
        assertHasPermission(work2File2Resc, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2014-02-17");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);
        Bag work3Bag = model.getBag(workResc3);
        testHelper.assertGroupedWorkPopulatedInSip(workResc3, dirManager, model, "607", false,
                stagingLocs.get(3), stagingLocs.get(4));
        Resource work3File1Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(3));
        assertHasPermission(work3File1Resc, CdrAcl.canViewMetadata);
        Resource work3File2Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(4));
        assertHasPermission(work3File2Resc, CdrAcl.canViewMetadata);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsGroupedWorkPermissions() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif");
        setupGroupIndex();
        // permissions must be set after the grouping
        testHelper.generateAllPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        Resource work1File1Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(0));
        assertHasPermission(work1File1Resc, CdrAcl.canViewMetadata);
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        assertHasPermission(work1File2Resc, CdrAcl.canViewMetadata);
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertHasPermission(workResc4, CdrAcl.canViewMetadata);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithCompoundObjectsFilePermissions() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.generateFilePermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        setupDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        List<Path> stagingLocs = testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        // Verify that compound object works do not have permissions
        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2012-05-18");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "216");
        assertDoesNotHavePermission(workResc1, CdrAcl.canViewMetadata);

        // Verify that the children of the compound object have permissions
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2014-01-17");
        Bag work2Bag = model.getBag(workResc2);
        testHelper.assertGroupedWorkPopulatedInSip(workResc2, dirManager, model, "604", false,
                stagingLocs.get(1), stagingLocs.get(2));
        Resource work2File1Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(1));
        assertHasPermission(work2File1Resc, CdrAcl.canViewMetadata);
        Resource work2File2Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(2));
        assertHasPermission(work2File2Resc, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2014-02-17");
        Bag work3Bag = model.getBag(workResc3);
        testHelper.assertGroupedWorkPopulatedInSip(workResc3, dirManager, model, "607", false,
                stagingLocs.get(3), stagingLocs.get(4));
        Resource work3File1Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(3));
        assertHasPermission(work3File1Resc, CdrAcl.canViewMetadata);
        Resource work3File2Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(4));
        assertHasPermission(work3File2Resc, CdrAcl.canViewMetadata);
        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsGroupedWorkFilePermissions() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif");
        setupGroupIndex();
        // permissions must be set after the grouping
        testHelper.generateFilePermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        // Assert that children of grouped work have permissions
        Resource work1File1Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(0));
        assertHasPermission(work1File1Resc, CdrAcl.canViewMetadata);
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        assertHasPermission(work1File2Resc, CdrAcl.canViewMetadata);
        // Assert that grouped works do not have permissions
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertDoesNotHavePermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertDoesNotHavePermission(workResc3, CdrAcl.canViewMetadata);
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertDoesNotHavePermission(workResc4, CdrAcl.canViewMetadata);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithCompoundObjectsWorkPermissions() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.generateWorkPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);
        setupDescriptions();
        var sourceOptions = testHelper.makeSourceFileOptions(testHelper.getSourceFilesBasePath());
        sourceOptions.setExportField("filena");
        List<Path> stagingLocs = testHelper.populateSourceFiles(sourceOptions, "nccg_ck_09.tif", "nccg_ck_1042-22_v1.tif",
                "nccg_ck_1042-22_v2.tif", "nccg_ck_549-4_v1.tif", "nccg_ck_549-4_v2.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2012-05-18");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "216");
        assertHasPermission(workResc1, CdrAcl.canViewMetadata);

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2014-01-17");
        Bag work2Bag = model.getBag(workResc2);
        testHelper.assertGroupedWorkPopulatedInSip(workResc2, dirManager, model, "604", false,
                stagingLocs.get(1), stagingLocs.get(2));
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource work2File1Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(1));
        assertDoesNotHavePermission(work2File1Resc, CdrAcl.canViewMetadata);
        Resource work2File2Resc = testHelper.findChildByStagingLocation(work2Bag, stagingLocs.get(2));
        assertDoesNotHavePermission(work2File2Resc, CdrAcl.canViewMetadata);

        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2014-02-17");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);
        Bag work3Bag = model.getBag(workResc3);
        testHelper.assertGroupedWorkPopulatedInSip(workResc3, dirManager, model, "607", false,
                stagingLocs.get(3), stagingLocs.get(4));
        Resource work3File1Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(3));
        assertDoesNotHavePermission(work3File1Resc, CdrAcl.canViewMetadata);
        Resource work3File2Resc = testHelper.findChildByStagingLocation(work3Bag, stagingLocs.get(4));
        assertDoesNotHavePermission(work3File2Resc, CdrAcl.canViewMetadata);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsGroupedWorkWorkPermissions() throws Exception {
        testHelper.indexExportData("grouped_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_185_E.tif", "276_183_E.tif", "276_203_E.tif",
                "276_241_E.tif", "276_245a_E.tif");
        setupGroupIndex();
        // permissions must be set after the grouping
        testHelper.generateWorkPermissionsMapping(UserRole.canViewMetadata, UserRole.canViewMetadata);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(4, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        Bag work1Bag = model.getBag(workResc1);
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        // Assert that children of grouped work do not have permissions
        Resource work1File1Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(0));
        assertDoesNotHavePermission(work1File1Resc, CdrAcl.canViewMetadata);
        Resource work1File2Resc = testHelper.findChildByStagingLocation(work1Bag, stagingLocs.get(1));
        assertDoesNotHavePermission(work1File2Resc, CdrAcl.canViewMetadata);

        // Assert that grouped worked have permissions
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(2), null, "27");
        assertHasPermission(workResc2, CdrAcl.canViewMetadata);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-09");
        assertHasPermission(workResc3, CdrAcl.canViewMetadata);
        Resource workResc4 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-10");
        assertHasPermission(workResc4, CdrAcl.canViewMetadata);

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithStreamingUrl() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Bag workResc1Bag = model.getBag(workResc1);
        List<RDFNode> workResc1Children = workResc1Bag.iterator().toList();
        assertEquals(1, workResc1Children.size());
        Resource workResc1FileObj = workResc1Children.get(0).asResource();
        assertFalse(workResc1FileObj.hasProperty(STREAMING_URL, "https://durastream.lib.unc.edu/player?" +
                "spaceId=open-hls&filename=gilmer_recording-playlist.m3u8"));

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Bag workResc2Bag = model.getBag(workResc2);
        List<RDFNode> workResc2Children = workResc2Bag.iterator().toList();
        assertEquals(1, workResc2Children.size());
        Resource workResc2FileObj = workResc2Children.get(0).asResource();
        assertTrue(workResc2FileObj.hasProperty(STREAMING_URL, "https://durastream.lib.unc.edu/player?" +
                "spaceId=open-hls&filename=gilmer_video-playlist.m3u8"));
        assertTrue(workResc2FileObj.hasProperty(STREAMING_TYPE, "video"));

        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        Bag workResc3Bag = model.getBag(workResc3);
        List<RDFNode> workResc3Children = workResc3Bag.iterator().toList();
        assertEquals(1, workResc3Children.size());
        Resource workResc3FileObj = workResc3Children.get(0).asResource();
        assertTrue(workResc3FileObj.hasProperty(STREAMING_URL, "https://durastream.lib.unc.edu/player?" +
                "spaceId=open-hls&filename=gilmer_recording-playlist.m3u8"));
        assertTrue(workResc3FileObj.hasProperty(STREAMING_TYPE, "sound"));

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithAltText() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        List<Path> accessLocs = testHelper.populateAccessFiles("276_182_E.tif", "276_203_E.tif");
        writeAltTextCsv(altTextMappingBody("25,alt text"));

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), accessLocs.get(0), "25");
        Resource work1FileResc = getOnlyChildOf(workResc1);
        testHelper.assertAltTextPresent(dirManager, PIDs.get(work1FileResc.getURI()), "alt text");
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), accessLocs.get(1),  "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithFileSystemSourceTest() throws Exception {
        setupProject(null, MigrationProject.PROJECT_SOURCE_FILES);

        indexFromCsv(Path.of("src/test/resources/files/more_fields.csv"));
        testHelper.getDescriptionsService().generateDocuments(false);
        testHelper.getDescriptionsService().expandDescriptions();
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);

        var sourceOptions = testHelper.makeSourceFileOptions(Paths.get("src/test/resources/files/"));
        sourceOptions.setExportField("filename");
        testHelper.getSourceFileService().generateMapping(sourceOptions);

        var currentDate = Instant.now().atZone(ZoneOffset.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE);

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());
        System.out.println(model);

        // Verify that all the works were generated and given a timestamp of today
        for (var childNode : depBagChildren) {
            var childResc = childNode.asResource();
            assertTrue(childResc.hasLiteral(CdrDeposit.createTime, currentDate + "T00:00:00.000Z"));
            var childBag = model.getBag(childResc);
            var childBagChildren = childBag.iterator().toList();
            assertEquals(1, childBagChildren.size());
            var fileResc = childBagChildren.get(0).asResource();
            assertTrue(fileResc.hasProperty(RDF.type, Cdr.FileObject));
        }
    }

    @Test
    public void generateSipsWithAspaceRefIds() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        writeAspaceRefIdCsv(aspaceRefIdMappingBody("25,2817ec3c77e5ea9846d5c070d58d402b",
                "26,3817ec3c77e5ea9846d5c070d58d402b", "27,4817ec3c77e5ea9846d5c070d58d402b"));
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("gilmer_mods1.xml");

        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(makeOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = testHelper.createDepositDirectoryManager(sip);

        Model model = testHelper.getSipModel(sip);

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-23");
        testHelper.assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        assertTrue(workResc1.hasProperty(CdrAspace.refId, "2817ec3c77e5ea9846d5c070d58d402b"));

        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2005-11-24");
        testHelper.assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        assertTrue(workResc2.hasProperty(CdrAspace.refId, "3817ec3c77e5ea9846d5c070d58d402b"));

        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
        assertTrue(workResc3.hasProperty(CdrAspace.refId, "4817ec3c77e5ea9846d5c070d58d402b"));

        assertPersistedSipInfoMatches(sip);
    }

    public void indexFromCsv(Path csvPath) throws Exception {
        CdmFieldInfo csvExportFields = testHelper.getFieldService().retrieveFieldsFromCsv(csvPath);
        testHelper.getFieldService().persistFieldsToProject(project, csvExportFields);
        project.getProjectProperties().setExportedDate(Instant.now());

        CdmIndexOptions options = new CdmIndexOptions();
        options.setCsvFile(csvPath);
        options.setForce(false);

        testHelper.getIndexService().createDatabase(options);
        testHelper.getIndexService().indexAllFromCsv(options);
        ProjectPropertiesSerialization.write(project);
    }

    private void solrResponseWithPid() throws Exception {
        QueryResponse testResponse1 = new QueryResponse();
        SolrDocument testDocument1 = new SolrDocument();
        testDocument1.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID);
        SolrDocumentList testList1 = new SolrDocumentList();
        testList1.add(testDocument1);
        testResponse1.setResponse(new NamedList<>(Map.of("response", testList1)));

        QueryResponse testResponse2 = new QueryResponse();
        SolrDocument testDocument2 = new SolrDocument();
        testDocument2.addField(ArchivalDestinationsService.PID_KEY, DEST_UUID2);
        SolrDocumentList testList2 = new SolrDocumentList();
        testList2.add(testDocument2);
        testResponse2.setResponse(new NamedList<>(Map.of("response", testList2)));

        when(solrClient.query(any())).thenAnswer(invocation -> {
            var query = invocation.getArgument(0, SolrQuery.class);
            var solrQ = query.get("q");
            if (solrQ.equals(ArchivalDestinationsService.COLLECTION_ID + ":group1")) {
                return testResponse1;
            } else {
                return testResponse2;
            }
        });
    }

    private SipGenerationOptions makeOptions() {
        return makeOptions(false);
    }

    private SipGenerationOptions makeOptions(boolean force) {
        SipGenerationOptions options = new SipGenerationOptions();
        options.setUsername(USERNAME);
        options.setForce(force);
        return options;
    }

    private GroupMappingSyncOptions makeDefaultSyncOptions() {
        var options = new GroupMappingSyncOptions();
        options.setSortField("file");
        return options;
    }

    private void setupGroupIndex() throws Exception {
        GroupMappingOptions groupOptions = new GroupMappingOptions();
        groupOptions.setGroupFields(Arrays.asList("groupa"));
        GroupMappingService groupService = testHelper.getGroupMappingService();
        groupService.generateMapping(groupOptions);
        groupService.syncMappings(makeDefaultSyncOptions());
    }

    private void setupDescriptions() throws IOException {
        DescriptionsService descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        descriptionsService.generateDocuments(false);
        descriptionsService.expandDescriptions();
    }

    private void assertPersistedSipInfoMatches(MigrationSip expectedSip) {
        MigrationSip sipInfo = service.loadSipInfo(expectedSip.getSipPath());
        assertEquals(expectedSip.getDepositPid(), sipInfo.getDepositPid());
        assertEquals(expectedSip.getDestinationPid(), sipInfo.getDestinationPid());
        assertEquals(expectedSip.getNewCollectionLabel(), sipInfo.getNewCollectionLabel());
        assertEquals(expectedSip.getNewCollectionPid(), sipInfo.getNewCollectionPid());
    }

    private void assertRedirectMappingRowContentIsCorrect(CSVRecord row, MigrationProject project, String objectId) {
        assertEquals(project.getProjectProperties().getCdmCollectionId(), row.get("cdm_collection_id"));
        assertEquals(objectId, row.get("cdm_object_id"));
        assertFalse(StringUtils.isBlank(row.get("boxc_object_id")));
        assertFalse(StringUtils.isBlank(row.get("boxc_file_id")));
    }

    private void assertRedirectMappingRowContentNoFileId(CSVRecord row, MigrationProject project, String objectId) {
        assertEquals(project.getProjectProperties().getCdmCollectionId(), row.get("cdm_collection_id"));
        assertEquals(objectId, row.get("cdm_object_id"));
        assertFalse(StringUtils.isBlank(row.get("boxc_object_id")));
        assertTrue(StringUtils.isBlank(row.get("boxc_file_id")));
    }

    private void assertRedirectMappingCollectionRowContentIsCorrect(CSVRecord row, MigrationProject project, String boxcObjectId) {
        assertEquals(project.getProjectProperties().getCdmCollectionId(), row.get("cdm_collection_id"));
        assertTrue(StringUtils.isBlank(row.get("cdm_object_id")));
        assertEquals(boxcObjectId, row.get("boxc_object_id"));
        assertTrue(StringUtils.isBlank(row.get("boxc_file_id")));
    }

    private void assertHasPermission(Resource resource, Property permission) {
        assertTrue(resource.hasProperty(permission, PUBLIC_PRINC));
        assertTrue(resource.hasProperty(permission, AUTHENTICATED_PRINC));
    }

    private void assertDoesNotHavePermission(Resource resource, Property permission) {
        assertFalse(resource.hasProperty(permission, PUBLIC_PRINC));
        assertFalse(resource.hasProperty(permission, AUTHENTICATED_PRINC));
    }

    private void assertPermissionsUnassigned(Resource resource) {
        String message = "Expected no permissions to be assigned, but properties were " + convertResourcePropertiesToString(resource);
        assertFalse(resource.hasProperty(CdrAcl.canViewMetadata), message);
        assertFalse(resource.hasProperty(CdrAcl.canViewAccessCopies), message);
        assertFalse(resource.hasProperty(CdrAcl.canViewOriginals), message);
        assertFalse(resource.hasProperty(CdrAcl.canViewReducedQuality), message);
        assertFalse(resource.hasProperty(CdrAcl.none), message);
    }

    private Resource getOnlyChildOf(Resource resource) {
        List<RDFNode> children = getChildrenOfBag(resource);
        assertEquals(1, children.size());
        return children.get(0).asResource();
    }

    private List<RDFNode> getChildrenOfBag(Resource resource) {
        Bag bag = resource.getModel().getBag(resource);
        return bag.iterator().toList();
    }

    private String convertResourcePropertiesToString(Resource resource) {
        return resource.listProperties().toList().stream()
                .map(Statement::toString)
                .collect(Collectors.joining("\n"));
    }

    private String altTextMappingBody(String... rows) {
        return String.join(",", AltTextInfo.CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeAltTextCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAltTextMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    private String aspaceRefIdMappingBody(String... rows) {
        return String.join(",", AspaceRefIdInfo.BLANK_CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void writeAspaceRefIdCsv(String mappingBody) throws IOException {
        FileUtils.write(project.getAspaceRefIdMappingPath().toFile(),
                mappingBody, StandardCharsets.UTF_8);
        project.getProjectProperties().setAspaceRefIdMappingsUpdatedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }
}
