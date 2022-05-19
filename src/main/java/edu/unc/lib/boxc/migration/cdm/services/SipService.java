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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.unc.lib.boxc.deposit.impl.model.DepositModelHelpers;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationSipEntry;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SourceFileMapping;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;
import edu.unc.lib.boxc.model.api.DatastreamType;
import edu.unc.lib.boxc.model.api.SoftwareAgentConstants.SoftwareAgent;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.model.api.rdf.Cdr;
import edu.unc.lib.boxc.model.api.rdf.CdrDeposit;
import edu.unc.lib.boxc.model.api.rdf.Premis;
import edu.unc.lib.boxc.model.fcrepo.ids.AgentPids;
import edu.unc.lib.boxc.operations.api.events.PremisLogger;
import edu.unc.lib.boxc.operations.api.events.PremisLoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;
import static edu.unc.lib.boxc.model.api.DatastreamType.ORIGINAL_FILE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for generation and access to migration SIPs
 *
 * @author bbpennel
 */
public class SipService {
    private static final Logger log = getLogger(SipService.class);
    public static final String SIP_TDB_PATH = "tdb_model";
    public static final String MODEL_EXPORT_NAME = "model.n3";
    public static final String SIP_INFO_NAME = "sip_info.json";
    private static final ObjectReader SIP_INFO_READER = new ObjectMapper().readerFor(MigrationSip.class);
    private static final ObjectWriter SIP_INFO_WRITER = new ObjectMapper().writerFor(MigrationSip.class);

    private CdmIndexService indexService;
    private SourceFileService sourceFileService;
    private AccessFileService accessFileService;
    private DescriptionsService descriptionsService;
    private RedirectMappingService redirectMappingService;
    private PremisLoggerFactory premisLoggerFactory;
    private MigrationProject project;
    private PIDMinter pidMinter;

    private Map<String, DestinationSipEntry> cdmId2DestMap;
    private List<DestinationSipEntry> destEntries = new ArrayList<>();

    public SipService() {
    }

    /**
     * Generate SIPS for each destination mapped in this project
     * @return list of sip objects generated
     */
    public List<MigrationSip> generateSips(SipGenerationOptions options) {
        validateProjectState();
        initializeDestinations(options);
        redirectMappingService = new RedirectMappingService(project);

        Connection conn = null;
        try {
            redirectMappingService.init();
            WorkGeneratorFactory workGeneratorFactory = new WorkGeneratorFactory();
            workGeneratorFactory.options = options;
            workGeneratorFactory.sourceFilesInfo = sourceFileService.loadMappings();
            try {
                workGeneratorFactory.accessFilesInfo = accessFileService.loadMappings();
            } catch (NoSuchFileException e) {
                log.debug("No access mappings file, no access files will be added to the SIP");
            }
            conn = indexService.openDbConnection();
            workGeneratorFactory.conn = conn;

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + "," + CdmFieldInfo.CDM_CREATED
                        + "," + CdmIndexService.ENTRY_TYPE_FIELD
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.PARENT_ID_FIELD + " is null");
            while (rs.next()) {
                String cdmId = rs.getString(1);
                String cdmCreated = rs.getString(2) + "T00:00:00.000Z";
                String entryType = rs.getString(3);

                WorkGenerator workGen = workGeneratorFactory.create(cdmId, cdmCreated, entryType);
                try {
                    workGen.generate();
                } catch (SkipObjectException e) {
                    // Skipping
                }
            }

            // Finalize all of the SIPs by closing and exporting their models
            List<MigrationSip> sips = new ArrayList<>();
            for (DestinationSipEntry entry : destEntries) {
                entry.commitModel();
                exportDepositModel(entry);
                MigrationSip sip = new MigrationSip(entry);
                sips.add(sip);
                // Serialize the SIP info out to file
                SIP_INFO_WRITER.writeValue(sip.getSipPath().resolve(SIP_INFO_NAME).toFile(), sip);
                // Cleanup the TDB directory not that it has been exported
                try {
                    FileUtils.deleteDirectory(entry.getTdbPath().toFile());
                } catch (IOException e) {
                    log.warn("Failed to cleanup TDB directory", e);
                }
            }
            redirectMappingService.addCollectionRow(sips);
            project.getProjectProperties().setSipsGeneratedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);

            return sips;
        } catch (SQLException | IOException e) {
            throw new MigrationException("Failed to generate SIP", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
            destEntries.stream().forEach(DestinationSipEntry::close);
            redirectMappingService.closeCsv();
        }
    }

    /**
     * Factory which produces WorkGenerator objects
     * @author bbpennel
     */
    private class WorkGeneratorFactory {
        private SourceFilesInfo sourceFilesInfo;
        private SourceFilesInfo accessFilesInfo;
        private Connection conn;
        private SipGenerationOptions options;

        public WorkGenerator create(String cdmId, String cdmCreated, String entryType) {
            WorkGenerator gen;
            if (CdmIndexService.ENTRY_TYPE_GROUPED_WORK.equals(entryType)) {
                gen = new GroupedWorkGenerator();
            } else if (CdmIndexService.ENTRY_TYPE_COMPOUND_OBJECT.equals(entryType)) {
                gen = new MultiFileWorkGenerator();
            } else {
                gen = new WorkGenerator();
            }
            gen.accessFilesInfo = accessFilesInfo;
            gen.sourceFilesInfo = sourceFilesInfo;
            gen.conn = conn;
            gen.options = options;
            gen.destEntry = getDestinationEntry(cdmId);
            gen.cdmId = cdmId;
            gen.cdmCreated = cdmCreated;
            return gen;
        }
    }

    /**
     * WorkGenerator for works containing multiple files
     * @author bbpennel
     */
    private class MultiFileWorkGenerator extends WorkGenerator {
        @Override
        protected void generateWork() throws IOException {
            super.generateWork();
            // Add redirect mapping for compound object
            redirectMappingService.addRow(cdmId, workPid.getId(), null);
        }

        @Override
        protected List<PID> addChildObjects() throws IOException {
            try {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + "," + CdmFieldInfo.CDM_CREATED
                        + " from " + CdmIndexService.TB_NAME
                        + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + cdmId + "'");

                List<PID> childPids = new ArrayList<>();
                while (rs.next()) {
                    String cdmId = rs.getString(1);
                    String cdmCreated = rs.getString(2) + "T00:00:00.000Z";

                    SourceFileMapping sourceMapping = getSourceFileMapping(cdmId);
                    PID filePid = addFileObject(cdmId, cdmCreated, sourceMapping);

                    childPids.add(filePid);
                }
                return childPids;
            } catch (SQLException e) {
                throw new MigrationException(e);
            }
        }
    }

    /**
     * WorkGenerator for works created via grouping together CDM objects
     * @author bbpennel
     */
    private class GroupedWorkGenerator extends MultiFileWorkGenerator {
        @Override
        protected void generateWork() throws IOException {
            try {
                // Get the first child id in order to use its description
                String firstChild = getFirstChildId();
                Path expDescPath = getDescriptionPath(firstChild, false);

                log.info("Transforming group generated work {} to box-c work {}", cdmId, workPid.getId());
                workBag = model.createBag(workPid.getRepositoryPath());
                workBag.addProperty(RDF.type, Cdr.Work);
                workBag.addLiteral(CdrDeposit.createTime, cdmCreated);

                // Copy description to SIP
                copyDescriptionToSip(workPid, expDescPath);

                fileObjPids = addChildObjects();
            } catch (SQLException e) {
                throw new MigrationException("Failed to add child objects to " + workPid, e);
            }
        }

        private String getFirstChildId() throws SQLException {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID
                + " from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.PARENT_ID_FIELD + " = '" + cdmId + "'"
                + " limit 1");

            while (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    /**
     * Base generator for works which are constructed for standalone CDM objects
     * @author bbpennel
     */
    private class WorkGenerator {
        protected SourceFilesInfo sourceFilesInfo;
        protected SourceFilesInfo accessFilesInfo;
        protected Connection conn;
        protected SipGenerationOptions options;
        protected Model model;
        protected DestinationSipEntry destEntry;

        protected String cdmId;
        protected String cdmCreated;

        protected PID workPid;
        protected Bag workBag;
        protected List<PID> fileObjPids;

        public void generate() throws IOException, SQLException {
            workPid = pidMinter.mintContentPid();
            workBag = null;

            Bag destBag = destEntry.getDestinationBag();
            model = destBag.getModel();

            generateWork();

            destBag.add(workBag);

            // Generate migration PREMIS event
            addPremisEvent(destEntry, workPid, options);
            for (PID fileObjPid : fileObjPids) {
                addPremisEvent(destEntry, fileObjPid, options);
            }
        }

        protected void generateWork() throws IOException {
            Path expDescPath = getDescriptionPath(cdmId, false);


            log.info("Transforming CDM object {} to box-c work {}", cdmId, workPid.getId());
            workBag = model.createBag(workPid.getRepositoryPath());
            workBag.addProperty(RDF.type, Cdr.Work);
            workBag.addLiteral(CdrDeposit.createTime, cdmCreated);

            // Copy description to SIP
            copyDescriptionToSip(workPid, expDescPath);

            fileObjPids = addChildObjects();
        }

        protected List<PID> addChildObjects() throws IOException {
            SourceFileMapping sourceMapping = getSourceFileMapping(cdmId);
            return Collections.singletonList(addFileObject(cdmId, cdmCreated, sourceMapping));
        }

        protected void copyDescriptionToSip(PID pid, Path descPath) throws IOException {
            if (Files.notExists(descPath)) {
                return;
            }
            // Copy description to SIP
            Path sipDescPath = destEntry.getDepositDirManager().getModsPath(pid);
            Files.copy(descPath, sipDescPath);
        }

        protected Path getDescriptionPath(String cdmId, boolean allowMissing) {
            Path expDescPath = descriptionsService.getExpandedDescriptionFilePath(cdmId);
            if (Files.notExists(expDescPath)) {
                if (allowMissing) {
                    return null;
                }
                String message = "Cannot transform object " + cdmId + ", it does not have a MODS description";
                if (options.isForce()) {
                    outputLogger.info(message);
                    throw new SkipObjectException();
                } else {
                    throw new InvalidProjectStateException(message);
                }
            }
            return expDescPath;
        }

        protected SourceFileMapping getSourceFileMapping(String cdmId) {
            SourceFileMapping sourceMapping = sourceFilesInfo.getMappingByCdmId(cdmId);
            if (sourceMapping == null || sourceMapping.getSourcePath() == null) {
                String message = "Cannot transform object " + cdmId + ", no source file has been mapped";
                if (options.isForce()) {
                    outputLogger.info(message);
                    throw new SkipObjectException();
                } else {
                    throw new InvalidProjectStateException(message);
                }
            }
            return sourceMapping;
        }

        protected PID addFileObject(String cdmId, String cdmCreated, SourceFileMapping sourceMapping)
                throws IOException {
            // Create FileObject
            PID fileObjPid = pidMinter.mintContentPid();
            Resource fileObjResc = model.getResource(fileObjPid.getRepositoryPath());
            fileObjResc.addProperty(RDF.type, Cdr.FileObject);
            fileObjResc.addLiteral(CdrDeposit.createTime, cdmCreated);

            workBag.add(fileObjResc);
            workBag.addProperty(Cdr.primaryObject, fileObjResc);

            // Link source file
            Resource origResc = DepositModelHelpers.addDatastream(fileObjResc, ORIGINAL_FILE);
            Path sourcePath = sourceMapping.getSourcePath();
            origResc.addLiteral(CdrDeposit.stagingLocation, sourcePath.toUri().toString());
            origResc.addLiteral(CdrDeposit.label, sourcePath.getFileName().toString());

            // Link access file
            if (accessFilesInfo != null) {
                SourceFileMapping accessMapping = accessFilesInfo.getMappingByCdmId(cdmId);
                if (accessMapping != null && accessMapping.getSourcePath() != null) {
                    Resource accessResc = DepositModelHelpers.addDatastream(
                            fileObjResc, DatastreamType.ACCESS_SURROGATE);
                    accessResc.addLiteral(CdrDeposit.stagingLocation,
                            accessMapping.getSourcePath().toUri().toString());
                    String mimetype = accessFileService.getMimetype(accessMapping.getSourcePath());
                    accessResc.addLiteral(CdrDeposit.mimetype, mimetype);
                }
            }

            // add redirect mapping for this file
            redirectMappingService.addRow(cdmId, workPid.getId(), fileObjPid.getId());

            return fileObjPid;
        }
    }

    private void addPremisEvent(DestinationSipEntry destEntry, PID pid, SipGenerationOptions options) {
        Path premisPath = destEntry.getDepositDirManager().getPremisPath(pid);
        PremisLogger premisLogger = premisLoggerFactory.createPremisLogger(pid, premisPath.toFile());
        premisLogger.buildEvent(Premis.Ingestion)
                .addEventDetail("Object migrated as a part of the CONTENTdm to Box-c 5 migration")
                .addSoftwareAgent(AgentPids.forSoftware(SoftwareAgent.cdmToBxcMigrationUtil))
                .addAuthorizingAgent(AgentPids.forPerson(options.getUsername()))
                .writeAndClose();
    }

    private void exportDepositModel(DestinationSipEntry entry) throws IOException {
        Model model = entry.getDepositModelManager().getReadModel(entry.getDepositPid());
        Path modelExportPath = entry.getDepositDirManager().getDepositDir().resolve(MODEL_EXPORT_NAME);
        Writer writer = Files.newBufferedWriter(modelExportPath);
        model.write(writer, "N3");
        entry.getDepositModelManager().close();
    }

    private void validateProjectState() {
        MigrationProjectProperties props = project.getProjectProperties();
        if (props.getIndexedDate() == null) {
            throw new InvalidProjectStateException("Exported data must be indexed");
        }
        if (props.getDestinationsGeneratedDate() == null) {
            throw new InvalidProjectStateException("Destinations must be mapped");
        }
        if (props.getDescriptionsExpandedDate() == null) {
            throw new InvalidProjectStateException("Descriptions must be created and expanded");
        }
        if (props.getSourceFilesUpdatedDate() == null) {
            throw new InvalidProjectStateException("Source files must be mapped");
        }
    }

    private DestinationSipEntry getDestinationEntry(String cdmId) {
        DestinationSipEntry entry = cdmId2DestMap.get(cdmId);
        if (entry == null) {
            return cdmId2DestMap.get(DestinationsInfo.DEFAULT_ID);
        } else {
            return entry;
        }
    }

    private void initializeDestinations(SipGenerationOptions options) {
        try {
            DestinationsValidator validator = new DestinationsValidator();
            validator.setProject(project);
            List<String> errors = validator.validateMappings(options.isForce());
            if (!errors.isEmpty()) {
                throw new MigrationException("Invalid destination mappings file, encountered the following errors:\n"
                        + String.join("\n", errors));
            }

            DestinationsInfo destInfo = DestinationsService.loadMappings(project);
            // Cleanup previously generated SIPs
            FileUtils.deleteDirectory(project.getSipsPath().toFile());

            cdmId2DestMap = new HashMap<>();
            Map<String, DestinationSipEntry> destMap = new HashMap<>();
            for (DestinationMapping mapping : destInfo.getMappings()) {
                String key = !StringUtils.isBlank(mapping.getCollectionId()) ?
                        mapping.getCollectionId() : mapping.getDestination();
                // Retrieve existing destination entry or generate new one if this is first encounter
                DestinationSipEntry destEntry = destMap.computeIfAbsent(key, k -> {
                    PID depositPid = pidMinter.mintDepositRecordPid();
                    log.info("Initializing SIP for deposit {} to destination {}",
                            depositPid.getId(), mapping.getDestination());
                    DestinationSipEntry entry = new DestinationSipEntry(
                            depositPid, mapping, project.getSipsPath(), pidMinter);
                    entry.initializeDepositModel();
                    // Add description for new collection if one was provided
                    if (entry.getNewCollectionPid() != null) {
                        Path descPath = descriptionsService.getNewCollectionDescriptionPath(k);
                        if (Files.exists(descPath)) {
                            Path sipDescPath = entry.getDepositDirManager().getModsPath(entry.getNewCollectionPid());
                            try {
                                Files.copy(descPath, sipDescPath);
                            } catch (IOException e) {
                                throw new MigrationException("Failed to copy description", e);
                            }
                        }
                        addPremisEvent(entry, entry.getNewCollectionPid(), options);
                    }
                    destEntries.add(entry);
                    return entry;
                });

                cdmId2DestMap.put(mapping.getId(), destEntry);
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to load destination mappings", e);
        }
    }

    /**
     * @param sipPath path of the SIP directory
     * @return Deserialized SIP info for the provided SIP
     */
    public MigrationSip loadSipInfo(Path sipPath) {
        Path infoPath = sipPath.resolve(SIP_INFO_NAME);
        try {
            return SIP_INFO_READER.readValue(infoPath.toFile());
        } catch (IOException e) {
            throw new MigrationException("Unable to read sip info at path " + infoPath, e);
        }
    }

    /**
     * @return List of information about SIPs in this project
     */
    public List<MigrationSip> listSips() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(project.getSipsPath())) {
            List<MigrationSip> sips = new ArrayList<>();
            for (Path sipPath : stream) {
                sips.add(loadSipInfo(sipPath));
            }
            return sips;
        } catch (IOException e) {
            throw new MigrationException("Failed to list SIPs", e);
        }
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }

    public void setAccessFileService(AccessFileService accessFileService) {
        this.accessFileService = accessFileService;
    }

    public void setDescriptionsService(DescriptionsService descriptionsService) {
        this.descriptionsService = descriptionsService;
    }

    public void setPremisLoggerFactory(PremisLoggerFactory premisLoggerFactory) {
        this.premisLoggerFactory = premisLoggerFactory;
    }

    public void setPidMinter(PIDMinter pidMinter) {
        this.pidMinter = pidMinter;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public static class SkipObjectException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
