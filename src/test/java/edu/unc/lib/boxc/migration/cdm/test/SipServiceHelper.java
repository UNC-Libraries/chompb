package edu.unc.lib.boxc.migration.cdm.test;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.unc.lib.boxc.auth.api.UserRole;
import edu.unc.lib.boxc.migration.cdm.options.BoxctronFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.options.GenerateSourceFileMappingOptions;
import edu.unc.lib.boxc.migration.cdm.options.PermissionMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.AggregateFileMappingService;
import edu.unc.lib.boxc.migration.cdm.services.AltTextService;
import edu.unc.lib.boxc.migration.cdm.services.ArchivalDestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.AspaceRefIdService;
import edu.unc.lib.boxc.migration.cdm.services.BoxctronFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFileRetrievalService;
import edu.unc.lib.boxc.migration.cdm.services.ChompbConfigService;
import edu.unc.lib.boxc.migration.cdm.services.ExportObjectsService;
import edu.unc.lib.boxc.migration.cdm.services.FileIndexService;
import edu.unc.lib.boxc.migration.cdm.services.FindingAidReportService;
import edu.unc.lib.boxc.migration.cdm.services.GroupMappingService;
import edu.unc.lib.boxc.migration.cdm.services.PermissionsService;
import edu.unc.lib.boxc.migration.cdm.services.StreamingMetadataService;
import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.jdom2.Document;
import org.jdom2.Element;

import edu.unc.lib.boxc.common.xml.SecureXMLFactory;
import edu.unc.lib.boxc.deposit.impl.model.DepositDirectoryManager;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.DestinationMappingOptions;
import edu.unc.lib.boxc.migration.cdm.services.AccessFileService;
import edu.unc.lib.boxc.migration.cdm.services.CdmFieldService;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;
import edu.unc.lib.boxc.migration.cdm.services.DescriptionsService;
import edu.unc.lib.boxc.migration.cdm.services.DestinationsService;
import edu.unc.lib.boxc.migration.cdm.services.SipService;
import edu.unc.lib.boxc.migration.cdm.services.SourceFileService;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.model.api.SoftwareAgentConstants.SoftwareAgent;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDConstants;
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
public class SipServiceHelper {
    private final static Pattern DEPOSIT_ID_PATTERN = Pattern.compile(
            ".*Generated SIP for deposit with ID ([0-9a-f\\-]+).*", Pattern.DOTALL);
    private final static Pattern SIP_PATH_PATTERN = Pattern.compile(".*SIP path: ([^\\s]+).*", Pattern.DOTALL);
    private final static Pattern NEW_COLL_PATTERN =
            Pattern.compile(".*Added new collection ([^\\s]+) with box-c id ([^\\s]+).*", Pattern.DOTALL);

    private Path sourceFilesBasePath;
    private Path accessFilesBasePath;
    private MigrationProject project;
    private CdmFieldService fieldService;
    private SourceFileService sourceFileService;
    private AccessFileService accessFileService;
    private AltTextService altTextService;
    private AspaceRefIdService aspaceRefIdService;
    private AggregateFileMappingService aggregateFileMappingService;
    private AggregateFileMappingService aggregateBottomMappingService;
    private BoxctronFileService boxctronFileService;
    private DescriptionsService descriptionsService;
    private DestinationsService destinationsService;
    private ArchivalDestinationsService archivalDestinationsService;
    private CdmIndexService cdmIndexService;
    private FileIndexService fileIndexService;
    private FindingAidReportService findingAidReportService;
    private GroupMappingService groupMappingService;
    private PermissionsService permissionsService;
    private StreamingMetadataService streamingMetadataService;
    private PIDMinter pidMinter;
    private PremisLoggerFactoryImpl premisLoggerFactory;
    private ChompbConfigService.ChompbConfig chompbConfig;
    private ExportObjectsService exportObjectsService;

    public SipServiceHelper(MigrationProject project, Path filesBasePath) throws IOException {
        this.sourceFilesBasePath = new File(filesBasePath.toFile(), "source").toPath();
        this.accessFilesBasePath = new File(filesBasePath.toFile(), "access").toPath();
        this.project = project;
        chompbConfig = new ChompbConfigService.ChompbConfig();
        chompbConfig.setCdmEnvironments(CdmEnvironmentHelper.getTestMapping());
        chompbConfig.setBxcEnvironments(BxcEnvironmentHelper.getTestMapping());
        pidMinter = new RepositoryPIDMinter();
        premisLoggerFactory = new PremisLoggerFactoryImpl();
        premisLoggerFactory.setPidMinter(pidMinter);
        fieldService = new CdmFieldService();
        exportObjectsService = new ExportObjectsService();
        exportObjectsService.setProject(project);
        cdmIndexService = new CdmIndexService();
        cdmIndexService.setProject(project);
        cdmIndexService.setFieldService(fieldService);
        fileIndexService = new FileIndexService();
        fileIndexService.setProject(project);
        fileIndexService.setFieldService(fieldService);
        sourceFileService = new SourceFileService();
        sourceFileService.setIndexService(cdmIndexService);
        sourceFileService.setProject(project);
        accessFileService = new AccessFileService();
        accessFileService.setIndexService(cdmIndexService);
        accessFileService.setProject(project);
        altTextService = new AltTextService();
        altTextService.setIndexService(cdmIndexService);
        altTextService.setProject(project);
        aspaceRefIdService = new AspaceRefIdService();
        aspaceRefIdService.setIndexService(cdmIndexService);
        aspaceRefIdService.setProject(project);
        descriptionsService = new DescriptionsService();
        descriptionsService.setProject(project);
        destinationsService = new DestinationsService();
        destinationsService.setProject(project);
        archivalDestinationsService = new ArchivalDestinationsService();
        archivalDestinationsService.setProject(project);
        archivalDestinationsService.setIndexService(cdmIndexService);
        archivalDestinationsService.setDestinationsService(destinationsService);
        boxctronFileService = new BoxctronFileService();
        boxctronFileService.setProject(project);
        boxctronFileService.setIndexService(cdmIndexService);
        findingAidReportService = new FindingAidReportService();
        findingAidReportService.setProject(project);
        findingAidReportService.setIndexService(cdmIndexService);
        permissionsService = new PermissionsService();
        permissionsService.setProject(project);
        streamingMetadataService = new StreamingMetadataService();
        streamingMetadataService.setProject(project);
        streamingMetadataService.setFieldService(fieldService);
        streamingMetadataService.setIndexService(cdmIndexService);

        Files.createDirectories(project.getExportPath());
    }

    public SipService createSipsService() {
        SipService service = new SipService();
        service.setIndexService(cdmIndexService);
        service.setAccessFileService(accessFileService);
        service.setAltTextService(altTextService);
        service.setAspaceRefIdService(aspaceRefIdService);
        service.setSourceFileService(sourceFileService);
        service.setPidMinter(pidMinter);
        service.setDescriptionsService(descriptionsService);
        service.setPremisLoggerFactory(premisLoggerFactory);
        service.setProject(project);
        service.setChompbConfig(chompbConfig);
        service.setAggregateTopMappingService(getAggregateFileMappingService());
        service.setAggregateBottomMappingService(getAggregateBottomMappingService());
        service.setPermissionsService(permissionsService);
        service.setStreamingMetadataService(streamingMetadataService);
        return service;
    }

    public DepositDirectoryManager createDepositDirectoryManager(MigrationSip sip) {
        return new DepositDirectoryManager(sip.getDepositPid(), project.getSipsPath(), true);
    }

    public Resource getFirstSipFileInWork(Resource objResc, DepositDirectoryManager dirManager, Model depModel) {
        assertTrue(objResc.hasProperty(RDF.type, Cdr.Work));
        Bag workBag = depModel.getBag(objResc);
        List<RDFNode> workChildren = workBag.iterator().toList();
        assertEquals(1, workChildren.size());
        return workChildren.get(0).asResource();
    }

    public void assertObjectPopulatedInSip(Resource objResc, DepositDirectoryManager dirManager, Model depModel,
            Path stagingPath, Path accessPath, String cdmId) throws Exception {
        Resource fileObjResc = getFirstSipFileInWork(objResc, dirManager, depModel);
        assertTrue(fileObjResc.hasProperty(RDF.type, Cdr.FileObject));

        // Check for source file
        if (stagingPath != null) {
            Resource origResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamOriginal).getResource();
            assertTrue(origResc.hasLiteral(CdrDeposit.stagingLocation, stagingPath.toUri().toString()));
            assertTrue(origResc.hasLiteral(CdrDeposit.label, stagingPath.getFileName().toString()));
        }

        if (accessPath == null) {
            // Verify no access copy
            assertFalse(fileObjResc.hasProperty(CdrDeposit.hasDatastreamAccessSurrogate));
        } else {
            Resource accessResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamAccessSurrogate).getResource();
            accessResc.hasLiteral(CdrDeposit.stagingLocation, accessPath.toUri().toString());
            accessResc.hasLiteral(CdrDeposit.mimetype, "image/tiff");
        }

        PID workPid = PIDs.get(objResc.getURI());
        assertMigrationEventPresent(dirManager, workPid);
        PID fileObjPid = PIDs.get(fileObjResc.getURI());
        assertMigrationEventPresent(dirManager, fileObjPid);

        assertModsPresentWithCdmId(dirManager, workPid, cdmId);
    }

    /**
     *
     * @param objResc
     * @param dirManager
     * @param depModel
     * @param cdmId
     * @param withAccessCopies
     * @param filePaths if withAccessCopies is false, then this is a list of staging paths. Otherwise,
     *      this array should alternate between staging and access paths, staging first. If there is no
     *       access path for an object, then null must be provided.
     * @return the Bag resource representing the grouped work in the SIP's RDF model
     * @throws Exception
     */
    public void assertGroupedWorkPopulatedInSip(Resource objResc, DepositDirectoryManager dirManager,
                                                         Model depModel, String cdmId, boolean withAccessCopies,
                                                         Path... filePaths) throws Exception {
        List<Path> stagingPaths;
        List<Path> accessPaths = null;
        if (withAccessCopies) {
            // Split paths out into staging and access path lists
            stagingPaths = new ArrayList<>();
            accessPaths = new ArrayList<>();
            for (int i = 0; i < filePaths.length; i++) {
                if (i % 2 == 0) {
                    stagingPaths.add(filePaths[i]);
                } else {
                    accessPaths.add(filePaths[i]);
                }
            }
        } else {
            stagingPaths = Arrays.asList(filePaths);
        }

        assertTrue(objResc.hasProperty(RDF.type, Cdr.Work));
        Bag workBag = depModel.getBag(objResc);
        List<RDFNode> workChildren = workBag.iterator().toList();
        assertEquals(stagingPaths.size(), workChildren.size());

        for (int i = 0; i < stagingPaths.size(); i++) {
            Path stagingPath = stagingPaths.get(i);
            Resource fileObjResc = findChildByStagingLocation(workChildren, stagingPath);
            assertTrue(fileObjResc.hasProperty(RDF.type, Cdr.FileObject));

            Resource origResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamOriginal).getResource();
            assertTrue(origResc.hasLiteral(CdrDeposit.label, stagingPath.getFileName().toString()));

            if (withAccessCopies) {
                Path accessPath = accessPaths.get(i);
                if (accessPath == null) {
                    assertFalse(fileObjResc.hasProperty(CdrDeposit.hasDatastreamAccessSurrogate));
                } else {
                    Resource accessResc = fileObjResc.getProperty(CdrDeposit.hasDatastreamAccessSurrogate).getResource();
                    accessResc.hasLiteral(CdrDeposit.stagingLocation, accessPath.toUri().toString());
                    accessResc.hasLiteral(CdrDeposit.mimetype, "image/tiff");
                }
            }

            PID fileObjPid = PIDs.get(fileObjResc.getURI());
            assertMigrationEventPresent(dirManager, fileObjPid);
        }

        PID workPid = PIDs.get(objResc.getURI());
        assertMigrationEventPresent(dirManager, workPid);

        assertModsPresentWithCdmId(dirManager, workPid, cdmId);
    }

    public Resource findChildByStagingLocation(Bag workBag, Path stagingPath) {
        return findChildByStagingLocation(workBag.iterator().toList(), stagingPath);
    }

    /**
     * @param workChildren
     * @param stagingPath
     * @return resource of child from the provided list which has a stagingLocation matching the provided value
     */
    public Resource findChildByStagingLocation(List<RDFNode> workChildren, Path stagingPath) {
        String stagingUri = stagingPath.toUri().toString();
        return workChildren.stream().map(RDFNode::asResource)
                .filter(c -> c.getProperty(CdrDeposit.hasDatastreamOriginal).getResource()
                        .hasLiteral(CdrDeposit.stagingLocation, stagingUri))
                .findFirst().get();
    }

    public Resource getResourceByCreateTime(List<RDFNode> depBagChildren, String createTime) {
        return depBagChildren.stream()
                .map(RDFNode::asResource)
                .filter(c -> c.hasLiteral(CdrDeposit.createTime, createTime + "T00:00:00.000Z"))
                .findFirst().orElseGet(null);
    }

    public void indexExportData(String descPath) throws Exception {
        indexExportData(Paths.get("src/test/resources/gilmer_fields.csv"), descPath);
    }

    public void indexPdfExportData(String descPath) throws Exception {
        indexExportData(Paths.get("src/test/resources/pdf_fields.csv"), descPath);
    }

    public void indexExportData(Path fieldsPath, String descPath) throws Exception {
        CdmIndexOptions options = new CdmIndexOptions();
        options.setForce(true);
        Files.copy(fieldsPath, project.getFieldsPath(), REPLACE_EXISTING);
        Files.copy(Paths.get("src/test/resources/descriptions/" + descPath + "/index/description/desc.all"),
                CdmFileRetrievalService.getDescAllPath(project), REPLACE_EXISTING);
        // Copy over any associate CPD files
        var cpdsSrc = Paths.get("src/test/resources/descriptions/" + descPath + "/image/");
        var cpdsDest = CdmFileRetrievalService.getExportedCpdsPath(project);
        Files.createDirectories(cpdsDest);
        if (Files.isDirectory(cpdsSrc)) {
            Files.list(cpdsSrc).forEach(cpdFile -> {
                try {
                    Files.copy(cpdFile, cpdsDest.resolve(cpdFile.getFileName()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        project.getProjectProperties().setExportedDate(Instant.now());
        cdmIndexService.createDatabase(options);
        cdmIndexService.indexAll();
        ProjectPropertiesSerialization.write(project);
    }

    public void generateDefaultDestinationsMapping(String defDest, String defColl) throws Exception {
        DestinationMappingOptions options = new DestinationMappingOptions();
        options.setDefaultDestination(defDest);
        options.setDefaultCollection(defColl);
        destinationsService.generateMapping(options);
    }

    public void generateArchivalCollectionDestinationMapping(String defDest, String defColl, String fieldName) throws Exception {
        DestinationMappingOptions options = new DestinationMappingOptions();
        options.setDefaultDestination(defDest);
        options.setDefaultCollection(defColl);
        options.setFieldName(fieldName);
        archivalDestinationsService.addArchivalCollectionMappings(options);
    }

    public void generateDefaultPermissionsMapping(UserRole everyone, UserRole authenticated) throws Exception {
        PermissionMappingOptions options = new PermissionMappingOptions();
        options.setWithDefault(true);
        options.setEveryone(everyone);
        options.setAuthenticated(authenticated);
        permissionsService.generatePermissions(options);
    }

    public void generateAllPermissionsMapping(UserRole everyone, UserRole authenticated) throws Exception {
        PermissionMappingOptions options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setWithWorks(true);
        options.setEveryone(everyone);
        options.setAuthenticated(authenticated);
        permissionsService.generatePermissions(options);
    }

    public void generateFilePermissionsMapping(UserRole everyone, UserRole authenticated) throws Exception {
        PermissionMappingOptions options = new PermissionMappingOptions();
        options.setWithFiles(true);
        options.setEveryone(everyone);
        options.setAuthenticated(authenticated);
        permissionsService.generatePermissions(options);
    }

    public void generateWorkPermissionsMapping(UserRole everyone, UserRole authenticated) throws Exception {
        PermissionMappingOptions options = new PermissionMappingOptions();
        options.setWithWorks(true);
        options.setEveryone(everyone);
        options.setAuthenticated(authenticated);
        permissionsService.generatePermissions(options);
    }

    public List<Path> populateSourceFiles(String... filenames) throws Exception {
        return populateSourceFiles(makeSourceFileOptions(sourceFilesBasePath), filenames);
    }

    public List<Path> populateSourceFiles(GenerateSourceFileMappingOptions options, String... filenames) throws Exception {
        List<Path> sourcePaths = Arrays.stream(filenames).map(this::addSourceFile).collect(Collectors.toList());
        sourceFileService.generateMapping(options);
        return sourcePaths;
    }

    public List<Path> populateAccessFiles(String... filenames) throws Exception {
        List<Path> sourcePaths = Arrays.stream(filenames).map(this::addAccessFile).collect(Collectors.toList());
        accessFileService.generateMapping(makeSourceFileOptions(accessFilesBasePath));
        return sourcePaths;
    }

    public void populateBoxctronFiles(String boxctronPath1, String boxctronPath2) throws Exception {
        boxctronWriteCsv(boxctronMappingBody(boxctronPath1 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\",",
                boxctronPath2 + ",1,0.9,\"[0.0, 0.9, 1.0, 1.0]\","));
        BoxctronFileMappingOptions options = new BoxctronFileMappingOptions();
        options.setUpdate(true);
        boxctronFileService.generateMapping(options);
    }

    private String boxctronMappingBody(String... rows) {
        return String.join(",", BoxctronFileService.BOXCTRON_CSV_HEADERS) + "\n"
                + String.join("\n", rows);
    }

    private void boxctronWriteCsv(String mappingBody) throws IOException {
        FileUtils.write(boxctronFileService.getVelocicroptorDataPath(project.getProjectPath()).toFile(),
                mappingBody, StandardCharsets.UTF_8);
    }

    public GenerateSourceFileMappingOptions makeSourceFileOptions(Path basePath) {
        GenerateSourceFileMappingOptions options = new GenerateSourceFileMappingOptions();
        options.setBasePath(basePath);
        options.setExportField("file");
        options.setFieldMatchingPattern("(.+)");
        options.setFilenameTemplate("$1");
        options.setForce(true);
        return options;
    }

    public Path addSourceFile(String relPath) {
        return addSourceFile(sourceFilesBasePath, relPath);
    }

    public Path addAccessFile(String relPath) {
        return addSourceFile(accessFilesBasePath, relPath);
    }

    public Path addSourceFile(Path basePath, String relPath) {
        Path srcPath = basePath.resolve(relPath);
        // Create parent directories in case they don't exist
        try {
            Files.createDirectories(srcPath.getParent());
            FileUtils.write(srcPath.toFile(), relPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return srcPath;
    }

    public void populateDescriptions(String... descCollFilenames) throws Exception {
        for (String filename : descCollFilenames) {
            Files.copy(Paths.get("src/test/resources/mods_collections/" + filename),
                    project.getDescriptionsPath().resolve(filename));
        }
        descriptionsService.expandDescriptions();
    }

    public void assertMigrationEventPresent(DepositDirectoryManager dirManager, PID pid) throws Exception {
        Model model = RDFModelUtil.createModel(Files.newInputStream(dirManager.getPremisPath(pid)), "N3");
        Resource objResc = model.getResource(pid.getRepositoryPath());
        List<Statement> generated = model.listStatements(null, Prov.generated, objResc).toList();
        List<Resource> eventRescs = generated.stream()
                .map(Statement::getSubject)
                .filter(eResc -> eResc.hasProperty(RDF.type, Premis.Ingestion))
                .collect(Collectors.toList());
        assertEquals(1, eventRescs.size(), "Only one event should be present");
        Resource migrationEventResc = eventRescs.get(0);
        assertTrue(migrationEventResc.hasProperty(Premis.note, "Object migrated as a part of the CONTENTdm to Box-c 5 migration"),
                "Missing migration event note");
        Resource agentResc = migrationEventResc.getProperty(Premis.hasEventRelatedAgentExecutor).getResource();
        assertNotNull(agentResc, "Migration agent not set");
        assertEquals(AgentPids.forSoftware(SoftwareAgent.cdmToBxcMigrationUtil).getRepositoryPath(),
                agentResc.getURI());
        Resource authResc = migrationEventResc.getProperty(Premis.hasEventRelatedAgentAuthorizor).getResource();
        assertNotNull(authResc, "Migration authorizer not set");
    }

    public void assertModsPresentWithCdmId(DepositDirectoryManager dirManager, PID pid, String cdmId)
            throws Exception {
        Path path = dirManager.getModsPath(pid);
        Document doc = SecureXMLFactory.createSAXBuilder().build(path.toFile());
        List<Element> children = doc.getRootElement().getChildren("identifier", JDOMNamespaceUtil.MODS_V3_NS);
        Element cdmIdEl = children.stream()
            .filter(e -> "local".equals(e.getAttributeValue("type"))
                    && DescriptionsService.CDM_NUMBER_LABEL.equals(e.getAttributeValue("displayLabel")))
            .findFirst().orElseGet(null);
        assertNotNull(cdmIdEl, "Did not find a CDM identifier field");
        assertEquals(cdmId, cdmIdEl.getText());
    }

    public void assertAltTextPresent(DepositDirectoryManager dirManager, PID pid, String altTextBody)
            throws Exception {
        Path path = dirManager.getAltTextPath(pid);
        assertTrue(Files.exists(path));
        assertEquals(altTextBody, Files.readString(path));
    }

    public Model getSipModel(MigrationSip sip) throws IOException {
        return RDFModelUtil.createModel(Files.newInputStream(sip.getModelPath()), "N3");
    }

    public MigrationSip extractSipFromOutput(String output) {
        MigrationSip sip = new MigrationSip();
        Matcher idMatcher = DEPOSIT_ID_PATTERN.matcher(output);
        assertTrue(idMatcher.matches(), "No id found, output was: " + output);
        String depositId = idMatcher.group(1);

        Matcher pathMatcher = SIP_PATH_PATTERN.matcher(output);
        assertTrue(pathMatcher.matches());
        Path sipPath = Paths.get(pathMatcher.group(1));
        assertTrue(Files.exists(sipPath));

        Matcher collMatcher = NEW_COLL_PATTERN.matcher(output);
        if (collMatcher.matches()) {
            sip.setNewCollectionLabel(collMatcher.group(1));
            sip.setNewCollectionId(collMatcher.group(2));
        }

        sip.setDepositPid(PIDs.get(PIDConstants.DEPOSITS_QUALIFIER, depositId));
        sip.setSipPath(sipPath);
        return sip;
    }

    public void initializeDefaultProjectState(String dest_uuid) throws Exception {
        indexExportData("mini_gilmer");
        generateDefaultDestinationsMapping(dest_uuid, null);
        populateDescriptions("gilmer_mods1.xml");
        populateSourceFiles("276_182_E.tif", "276_183_E.tif", "276_203_E.tif");
    }

    public void addSipsSubmitted() {
        project.getProjectProperties().getSipsSubmitted().add("Sips submitted!");
    }

    public Path getSourceFilesBasePath() {
        return sourceFilesBasePath;
    }

    public Path getAccessFilesBasePath() {
        return accessFilesBasePath;
    }

    public MigrationProject getProject() {
        return project;
    }

    public CdmFieldService getFieldService() {
        return fieldService;
    }

    public SourceFileService getSourceFileService() {
        return sourceFileService;
    }

    public AccessFileService getAccessFileService() {
        return accessFileService;
    }

    public AltTextService getAltTextService() {
        if (this.altTextService == null) {
            this.altTextService = new AltTextService();
            this.altTextService.setProject(project);
            this.altTextService.setIndexService(cdmIndexService);
        }
        return this.altTextService;
    }

    public AspaceRefIdService getAspaceRefIdService() {
        if (this.aspaceRefIdService == null) {
            this. aspaceRefIdService = new AspaceRefIdService();
            this.aspaceRefIdService.setProject(project);
            this.aspaceRefIdService.setIndexService(cdmIndexService);
        }
        return this.aspaceRefIdService;
    }

    public AggregateFileMappingService getAggregateFileMappingService() {
        if (this.aggregateFileMappingService == null) {
            this.aggregateFileMappingService = new AggregateFileMappingService(false);
            this.aggregateFileMappingService.setProject(project);
            this.aggregateFileMappingService.setIndexService(cdmIndexService);
        }
        return this.aggregateFileMappingService;
    }

    public AggregateFileMappingService getAggregateBottomMappingService() {
        if (this.aggregateBottomMappingService == null) {
            this.aggregateBottomMappingService = new AggregateFileMappingService(true);
            this.aggregateBottomMappingService.setProject(project);
            this.aggregateBottomMappingService.setIndexService(cdmIndexService);
        }
        return this.aggregateBottomMappingService;
    }

    public BoxctronFileService getBoxctronFileService() {
        if (this.boxctronFileService == null) {
            this.boxctronFileService = new BoxctronFileService();
            this.boxctronFileService.setProject(project);
            this.boxctronFileService.setIndexService(cdmIndexService);
        }
        return this.boxctronFileService;
    }

    public GroupMappingService getGroupMappingService() {
        if (this.groupMappingService == null) {
            this.groupMappingService = new GroupMappingService();
            this.groupMappingService.setFieldService(fieldService);
            this.groupMappingService.setIndexService(cdmIndexService);
            this.groupMappingService.setProject(project);
        }
        return this.groupMappingService;
    }

    public DescriptionsService getDescriptionsService() {
        return descriptionsService;
    }

    public DestinationsService getDestinationsService() {
        return destinationsService;
    }

    public ArchivalDestinationsService getArchivalDestinationsService() {
        return archivalDestinationsService;
    }

    public FindingAidReportService getFindingAidReportService() {
        return findingAidReportService;
    }

    public CdmIndexService getCdmIndexService() {
        return cdmIndexService;
    }

    public FileIndexService getFileIndexService() {
        return fileIndexService;
    }

    public StreamingMetadataService getStreamingMetadataService() {
        return streamingMetadataService;
    }

    public PIDMinter getPidMinter() {
        return pidMinter;
    }

    public PremisLoggerFactoryImpl getPremisLoggerFactory() {
        return premisLoggerFactory;
    }

    public ChompbConfigService.ChompbConfig getChompbConfig() {
        return chompbConfig;
    }
}
