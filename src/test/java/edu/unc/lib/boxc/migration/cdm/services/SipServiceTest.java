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

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.GroupMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.test.SipServiceHelper;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrAcl;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.jdom2.Document;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.AUTHENTICATED_PRINC;
import static edu.unc.lib.boxc.auth.api.AccessPrincipalConstants.PUBLIC_PRINC;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author bbpennel
 */
public class SipServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String USERNAME = "migr_user";
    private static final String DEST_UUID = "bfe93126-849a-43a5-b9d9-391e18ffacc6";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private MigrationProject project;
    private SipService service;
    private SipServiceHelper testHelper;

    @Before
    public void setup() throws Exception {
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.newFolder().toPath(), PROJECT_NAME, null, USERNAME);

        testHelper = new SipServiceHelper(project, tmpFolder.newFolder().toPath());
        service = testHelper.createSipsService();
    }

    @Test
    public void generateSipsDataNotIndexed() throws Exception {
        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Exported data must be indexed"));
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
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Destinations must be mapped"));
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
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Descriptions must be created"));
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
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Source files must be mapped"));
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
        assertTrue(collResc.hasProperty(CdrAcl.canViewOriginals, PUBLIC_PRINC));
        assertTrue(collResc.hasProperty(CdrAcl.canViewOriginals, AUTHENTICATED_PRINC));
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
        assertTrue(collResc.hasProperty(CdrAcl.canViewOriginals, PUBLIC_PRINC));
        assertTrue(collResc.hasProperty(CdrAcl.canViewOriginals, AUTHENTICATED_PRINC));
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
        testHelper.populateSourceFiles("276_182_E.tif", "276_203_E.tif");

        try {
            service.generateSips(makeOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("no source file has been mapped"));
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
        assertEquals(2, depBagChildren.size());

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
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("does not have a MODS description"));
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
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        GroupMappingOptions groupOptions = new GroupMappingOptions();
        groupOptions.setGroupField("groupa");
        GroupMappingService groupService = new GroupMappingService();
        groupService.setProject(project);
        groupService.setIndexService(testHelper.getIndexService());
        groupService.setFieldService(testHelper.getFieldService());
        groupService.generateMapping(groupOptions);
        groupService.syncMappings();

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
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", false,
                stagingLocs.get(0), stagingLocs.get(1));
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipsWithGroupedWorkWithAccessCopies() throws Exception {
        testHelper.indexExportData("mini_gilmer");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        testHelper.populateDescriptions("grouped_mods.xml");
        List<Path> stagingLocs = testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
        List<Path> accessLocs = testHelper.populateAccessFiles("276_182_E.tif", "276_203_E.tif");

        GroupMappingOptions groupOptions = new GroupMappingOptions();
        groupOptions.setGroupField("groupa");
        GroupMappingService groupService = new GroupMappingService();
        groupService.setProject(project);
        groupService.setIndexService(testHelper.getIndexService());
        groupService.setFieldService(testHelper.getFieldService());
        groupService.generateMapping(groupOptions);
        groupService.syncMappings();

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
        testHelper.assertGroupedWorkPopulatedInSip(workResc1, dirManager, model, "grp:groupa:group1", true,
                stagingLocs.get(0), accessLocs.get(0), stagingLocs.get(1), null);
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2005-12-08");
        testHelper.assertObjectPopulatedInSip(workResc3, dirManager, model,
                stagingLocs.get(2), accessLocs.get(1), "27");

        assertPersistedSipInfoMatches(sip);
    }

    @Test
    public void generateSipWithCompoundObjects() throws Exception {
        testHelper.indexExportData(Paths.get("src/test/resources/keepsakes_fields.csv"), "mini_keepsakes");
        testHelper.generateDefaultDestinationsMapping(DEST_UUID, null);
        DescriptionsService descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        descriptionsService.generateDocuments(false);
        descriptionsService.expandDescriptions();
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
        Resource workResc2 = testHelper.getResourceByCreateTime(depBagChildren, "2014-01-17");
        testHelper.assertGroupedWorkPopulatedInSip(workResc2, dirManager, model, "604", false,
                stagingLocs.get(1), stagingLocs.get(2));
        Resource workResc3 = testHelper.getResourceByCreateTime(depBagChildren, "2014-02-17");
        testHelper.assertGroupedWorkPopulatedInSip(workResc3, dirManager, model, "607", false,
                stagingLocs.get(3), stagingLocs.get(4));

        assertPersistedSipInfoMatches(sip);
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
        testHelper.populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");

        GroupMappingOptions groupOptions = new GroupMappingOptions();
        groupOptions.setGroupField("groupa");
        GroupMappingService groupService = new GroupMappingService();
        groupService.setProject(project);
        groupService.setIndexService(testHelper.getIndexService());
        groupService.setFieldService(testHelper.getFieldService());
        groupService.generateMapping(groupOptions);
        groupService.syncMappings();

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
        DescriptionsService descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        descriptionsService.generateDocuments(false);
        descriptionsService.expandDescriptions();
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
            assertRedirectMappingRowContentIsCorrect(rows.get(4), project, "605");
            assertRedirectMappingRowContentIsCorrect(rows.get(5), project, "606");
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

    private  SipGenerationOptions makeOptions() {
        return makeOptions(false);
    }

    private  SipGenerationOptions makeOptions(boolean force) {
        SipGenerationOptions options = new SipGenerationOptions();
        options.setUsername(USERNAME);
        options.setForce(force);
        return options;
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
}
