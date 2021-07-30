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

import static java.nio.file.StandardOpenOption.APPEND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.options.GenerateDestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.options.SourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.SipService.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.model.api.SoftwareAgentConstants.SoftwareAgent;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import edu.unc.lib.boxc.model.api.rdf.Premis;
import edu.unc.lib.boxc.model.api.rdf.Prov;
import edu.unc.lib.boxc.model.api.rdf.RDFModelUtil;
import edu.unc.lib.boxc.model.api.xml.JDOMNamespaceUtil;
import edu.unc.lib.boxc.model.fcrepo.ids.AgentPids;
import edu.unc.lib.boxc.model.fcrepo.ids.PIDs;
import edu.unc.lib.boxc.model.fcrepo.ids.RepositoryPIDMinter;
import edu.unc.lib.boxc.operations.impl.events.PremisLoggerFactoryImpl;

/**
 * @author bbpennel
 */
public class SipServiceTest {
    private static final String PROJECT_NAME = "proj";
    private static final String DEST_UUID = "bfe93126-849a-43a5-b9d9-391e18ffacc6";
    private static final String DEST_UUID2 = "8ae56bbc-400e-496d-af4b-3c585e20dba1";

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private Path sourceFilesBasePath;
    private Path accessFilesBasePath;
    private MigrationProject project;
    private CdmFieldService fieldService;
    private SourceFileService sourceFileService;
    private AccessFileService accessFileService;
    private DescriptionsService descriptionsService;
    private DestinationsService destinationsService;
    private CdmIndexService indexService;
    private PIDMinter pidMinter;
    private PremisLoggerFactoryImpl premisLoggerFactory;
    private SipService service;

    @Before
    public void setup() throws Exception {
        sourceFilesBasePath = tmpFolder.newFolder().toPath();
        project = MigrationProjectFactory.createMigrationProject(
                tmpFolder.newFolder().toPath(), PROJECT_NAME, null, "user");
        Files.createDirectories(project.getExportPath());

        pidMinter = new RepositoryPIDMinter();
        premisLoggerFactory = new PremisLoggerFactoryImpl();
        premisLoggerFactory.setPidMinter(pidMinter);
        fieldService = new CdmFieldService();
        indexService = new CdmIndexService();
        indexService.setProject(project);
        indexService.setFieldService(fieldService);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(indexService);
        sourceFileService.setProject(project);
        accessFileService = new AccessFileService();
        accessFileService.setIndexService(indexService);
        accessFileService.setProject(project);
        descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        destinationsService = new DestinationsService();
        destinationsService.setProject(project);

        service = new SipService();
        service.setIndexService(indexService);
        service.setAccessFileService(accessFileService);
        service.setSourceFileService(sourceFileService);
        service.setPidMinter(pidMinter);
        service.setDescriptionsService(descriptionsService);
        service.setPremisLoggerFactory(premisLoggerFactory);
        service.setProject(project);
    }

    @Test
    public void generateSipsDataNotIndexed() throws Exception {
        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Exported data must be indexed"));
        }
    }

    @Test
    public void generateSipsDestinationsNotGenerated() throws Exception {
        indexExportData("export_1.xml");
        populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");
        populateDescriptions("gilmer_mods1.xml");

        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Destinations must be mapped"));
        }
    }

    @Test
    public void generateSipsDescriptionsNotGenerated() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Descriptions must be created"));
        }
    }

    @Test
    public void generateSipsSourceFilesNotMapped() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");

        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Source files must be mapped"));
        }
    }

    @Test
    public void generateSipsSingleDestination() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(new SipGenerationOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = getResourceByCreateTime(depBagChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = getResourceByCreateTime(depBagChildren, "2005-11-24");
        assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = getResourceByCreateTime(depBagChildren, "2005-12-08");
        assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
    }

    @Test
    public void generateSipsNewCollectionDestination() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, "001234");
        populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(new SipGenerationOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(1, depBagChildren.size());

        Resource collResc = depBagChildren.iterator().next().asResource();
        assertTrue(collResc.hasProperty(RDF.type, Cdr.Collection));
        assertTrue(collResc.hasProperty(CdrDeposit.label, "001234"));
        Bag collBag = model.getBag(collResc);
        List<RDFNode> collChildren = collBag.iterator().toList();

        Resource workResc1 = getResourceByCreateTime(collChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = getResourceByCreateTime(collChildren, "2005-11-24");
        assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = getResourceByCreateTime(collChildren, "2005-12-08");
        assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");
    }

    @Test
    public void generateSipsWithAccessFiles() throws Exception {
        accessFilesBasePath = tmpFolder.newFolder().toPath();

        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");
        List<Path> accessLocs = populateAccessFiles("276_182_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(new SipGenerationOptions());
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(3, depBagChildren.size());

        Resource workResc1 = getResourceByCreateTime(depBagChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), accessLocs.get(0), "25");
        Resource workResc2 = getResourceByCreateTime(depBagChildren, "2005-11-24");
        assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
        Resource workResc3 = getResourceByCreateTime(depBagChildren, "2005-12-08");
        assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), accessLocs.get(1), "27");
    }

    @Test
    public void generateSipsMissingSourceFile() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        // Only populating 2 out of 3 source files expected from the export
        populateSourceFiles("276_182_E.tif", "276_203_E.tif");

        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("no source file has been mapped"));
        }
    }

    @Test
    public void generateSipsMissingSourceFileWithForce() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        // Only populating 2 out of 3 source files expected from the export
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_203_E.tif");

        SipGenerationOptions options = new SipGenerationOptions();
        options.setForce(true);

        List<MigrationSip> sips = service.generateSips(options);
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = getResourceByCreateTime(depBagChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc3 = getResourceByCreateTime(depBagChildren, "2005-12-08");
        assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(1), null, "27");
    }

    @Test
    public void generateSipsMissingDescription() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        // Deleting one description so that it is missing
        Files.delete(descriptionsService.getExpandedDescriptionFilePath("26"));
        populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        try {
            service.generateSips(new SipGenerationOptions());
            fail();
        } catch (InvalidProjectStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("does not have a MODS description"));
        }
    }

    @Test
    public void generateSipsMissingDescriptionWithForce() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        populateDescriptions("gilmer_mods1.xml");
        // Deleting one description so that it is missing
        Files.delete(descriptionsService.getExpandedDescriptionFilePath("27"));
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        SipGenerationOptions options = new SipGenerationOptions();
        options.setForce(true);

        List<MigrationSip> sips = service.generateSips(options);
        assertEquals(1, sips.size());
        MigrationSip sip = sips.get(0);

        assertTrue(Files.exists(sip.getSipPath()));

        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");

        Bag depBag = model.getBag(sip.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = getResourceByCreateTime(depBagChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc2 = getResourceByCreateTime(depBagChildren, "2005-11-24");
        assertObjectPopulatedInSip(workResc2, dirManager, model, stagingLocs.get(1), null, "26");
    }

    @Test
    public void generateSipsMultipleDestinations() throws Exception {
        indexExportData("export_1.xml");
        generateDefaultDestinationsMapping(DEST_UUID, null);
        // Inserting an extra destination which has one object mapped to it
        try (BufferedWriter writer = Files.newBufferedWriter(project.getDestinationMappingsPath(), APPEND)) {
            writer.write("26," + DEST_UUID2 + ",");
        }

        populateDescriptions("gilmer_mods1.xml");
        List<Path> stagingLocs = populateSourceFiles("276_182_E.tif", "276_183B_E.tif", "276_203_E.tif");

        List<MigrationSip> sips = service.generateSips(new SipGenerationOptions());
        assertEquals(2, sips.size());
        MigrationSip sip1 = sips.get(0);

        assertTrue(Files.exists(sip1.getSipPath()));
        DepositDirectoryManager dirManager = new DepositDirectoryManager(
                sip1.getDepositPid(), project.getSipsPath(), true);

        Model model = RDFModelUtil.createModel(Files.newInputStream(sip1.getModelPath()), "N3");

        Bag depBag = model.getBag(sip1.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren = depBag.iterator().toList();
        assertEquals(2, depBagChildren.size());

        Resource workResc1 = getResourceByCreateTime(depBagChildren, "2005-11-23");
        assertObjectPopulatedInSip(workResc1, dirManager, model, stagingLocs.get(0), null, "25");
        Resource workResc3 = getResourceByCreateTime(depBagChildren, "2005-12-08");
        assertObjectPopulatedInSip(workResc3, dirManager, model, stagingLocs.get(2), null, "27");

        MigrationSip sip2 = sips.get(1);

        assertTrue(Files.exists(sip2.getSipPath()));
        DepositDirectoryManager dirManager2 = new DepositDirectoryManager(
                sip2.getDepositPid(), project.getSipsPath(), true);

        Model model2 = RDFModelUtil.createModel(Files.newInputStream(sip2.getModelPath()), "N3");

        Bag depBag2 = model2.getBag(sip2.getDepositPid().getRepositoryPath());
        List<RDFNode> depBagChildren2 = depBag2.iterator().toList();
        assertEquals(1, depBagChildren2.size());

        Resource workResc2 = getResourceByCreateTime(depBagChildren2, "2005-11-24");
        assertObjectPopulatedInSip(workResc2, dirManager2, model2, stagingLocs.get(1), null, "26");
    }

    private void assertObjectPopulatedInSip(Resource objResc, DepositDirectoryManager dirManager, Model depModel,
            Path stagingPath, Path accessPath, String cdmId) throws Exception {
        assertTrue(objResc.hasProperty(RDF.type, Cdr.Work));
        Bag workBag = depModel.getBag(objResc);
        List<RDFNode> workChildren = workBag.iterator().toList();
        assertEquals(1, workChildren.size());
        Resource fileObjResc = workChildren.get(0).asResource();
        assertTrue(fileObjResc.hasProperty(RDF.type, Cdr.FileObject));
        assertTrue(workBag.hasProperty(Cdr.primaryObject, fileObjResc));

        // Check for source file
        Resource origResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamOriginal).getResource();
        origResc.hasLiteral(CdrDeposit.stagingLocation, stagingPath.toUri().toString());

        if (accessPath == null) {
            // Verify no access copy
            assertFalse(fileObjResc.hasProperty(CdrDeposit.hasDatastreamAccessCopy));
        } else {
            Resource accessResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamAccessCopy).getResource();
            accessResc.hasLiteral(CdrDeposit.stagingLocation, accessPath.toUri().toString());
            accessResc.hasLiteral(CdrDeposit.mimetype, "image/tiff");
        }


        PID workPid = PIDs.get(objResc.getURI());
        assertMigrationEventPresent(dirManager, workPid);

        assertModsPresentWithCdmId(dirManager, workPid, cdmId);
    }

    private Resource getResourceByCreateTime(List<RDFNode> depBagChildren, String createTime) {
        return depBagChildren.stream()
                .map(RDFNode::asResource)
                .filter(c -> c.hasLiteral(CdrDeposit.createTime, createTime + "T00:00:00.000Z"))
                .findFirst().orElseGet(null);
    }

    private void indexExportData(String... filenames) throws Exception {
        Files.copy(Paths.get("src/test/resources/gilmer_fields.csv"), project.getFieldsPath());
        for (String filename : filenames) {
            Files.copy(Paths.get("src/test/resources/sample_exports/" + filename),
                    project.getExportPath().resolve(filename));
        }
        project.getProjectProperties().setExportedDate(Instant.now());
        indexService.createDatabase(true);
        indexService.indexAll();
        ProjectPropertiesSerialization.write(project);
    }

    private void generateDefaultDestinationsMapping(String defDest, String defColl) throws Exception {
        GenerateDestinationMappingOptions options = new GenerateDestinationMappingOptions();
        options.setDefaultDestination(defDest);
        options.setDefaultCollection(defColl);
        destinationsService.generateMapping(options);
    }

    private List<Path> populateSourceFiles(String... filenames) throws Exception {
        List<Path> sourcePaths = Arrays.stream(filenames).map(this::addSourceFile).collect(Collectors.toList());
        sourceFileService.generateMapping(makeSourceFileOptions(sourceFilesBasePath));
        return sourcePaths;
    }

    private List<Path> populateAccessFiles(String... filenames) throws Exception {
        List<Path> sourcePaths = Arrays.stream(filenames).map(this::addAccessFile).collect(Collectors.toList());
        accessFileService.generateMapping(makeSourceFileOptions(accessFilesBasePath));
        return sourcePaths;
    }

    private SourceFileMappingOptions makeSourceFileOptions(Path basePath) {
        SourceFileMappingOptions options = new SourceFileMappingOptions();
        options.setBasePath(basePath);
        options.setExportField("file");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1");
        return options;
    }

    private Path addSourceFile(String relPath) {
        return addSourceFile(sourceFilesBasePath, relPath);
    }

    private Path addAccessFile(String relPath) {
        return addSourceFile(accessFilesBasePath, relPath);
    }

    private Path addSourceFile(Path basePath, String relPath) {
        Path srcPath = basePath.resolve(relPath);
        // Create parent directories in case they don't exist
        try {
            Files.createDirectories(srcPath.getParent());
            Files.createFile(srcPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return srcPath;
    }

    private void populateDescriptions(String... descCollFilenames) throws Exception {
        for (String filename : descCollFilenames) {
            Files.copy(Paths.get("src/test/resources/mods_collections/" + filename),
                    project.getDescriptionsPath().resolve(filename));
        }
        descriptionsService.expandDescriptions();
    }

    private void assertMigrationEventPresent(DepositDirectoryManager dirManager, PID pid) throws Exception {
        Model model = RDFModelUtil.createModel(Files.newInputStream(dirManager.getPremisPath(pid)), "N3");
        Resource objResc = model.getResource(pid.getRepositoryPath());
        List<Statement> generated = model.listStatements(null, Prov.generated, objResc).toList();
        List<Resource> eventRescs = generated.stream()
                .map(Statement::getSubject)
                .filter(eResc -> eResc.hasProperty(RDF.type, Premis.Ingestion))
                .collect(Collectors.toList());
        assertEquals("Only one event should be present", 1, eventRescs.size());
        Resource migrationEventResc = eventRescs.get(0);
        assertTrue("Missing migration event note",
                migrationEventResc.hasProperty(Premis.note, "Object migrated as a part of the CONTENTdm to Box-c 5 migration"));
        Resource agentResc = migrationEventResc.getProperty(Premis.hasEventRelatedAgentExecutor).getResource();
        assertNotNull("Migration agent not set", agentResc);
        assertEquals(AgentPids.forSoftware(SoftwareAgent.migrationUtil).getRepositoryPath(),
                agentResc.getURI());
    }

    private void assertModsPresentWithCdmId(DepositDirectoryManager dirManager, PID pid, String cdmId)
            throws Exception {
        Path path = dirManager.getModsPath(pid);
        Document doc = SecureXMLFactory.createSAXBuilder().build(path.toFile());
        List<Element> children = doc.getRootElement().getChildren("identifier", JDOMNamespaceUtil.MODS_V3_NS);
        Element cdmIdEl = children.stream()
            .filter(e -> "local".equals(e.getAttributeValue("type"))
                    && DescriptionsService.CDM_NUMBER_LABEL.equals(e.getAttributeValue("displayLabel")))
            .findFirst().orElseGet(null);
        assertNotNull("Did not find a CDM identifier field", cdmIdEl);
        assertEquals(cdmId, cdmIdEl.getText());
    }
}
