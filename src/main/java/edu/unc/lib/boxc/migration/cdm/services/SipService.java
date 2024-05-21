package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationSipEntry;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo;
import edu.unc.lib.boxc.migration.cdm.model.DestinationsInfo.DestinationMapping;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProjectProperties;
import edu.unc.lib.boxc.migration.cdm.model.MigrationSip;
import edu.unc.lib.boxc.migration.cdm.options.SipGenerationOptions;
import edu.unc.lib.boxc.migration.cdm.services.sips.CdmToDestMapper;
import edu.unc.lib.boxc.migration.cdm.services.sips.SipPremisLogger;
import edu.unc.lib.boxc.migration.cdm.services.sips.WorkGenerator;
import edu.unc.lib.boxc.migration.cdm.services.sips.WorkGeneratorFactory;
import edu.unc.lib.boxc.migration.cdm.util.DisplayProgressUtil;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import edu.unc.lib.boxc.migration.cdm.validators.DestinationsValidator;
import edu.unc.lib.boxc.model.api.ids.PID;
import edu.unc.lib.boxc.model.api.ids.PIDMinter;
import edu.unc.lib.boxc.operations.api.events.PremisLoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private PostMigrationReportService postMigrationReportService;
    private AggregateFileMappingService aggregateTopMappingService;
    private AggregateFileMappingService aggregateBottomMappingService;
    private SipPremisLogger sipPremisLogger;
    private MigrationProject project;
    private ChompbConfigService.ChompbConfig chompbConfig;
    private PermissionsService permissionsService;
    private StreamingMetadataService streamingMetadataService;
    private PIDMinter pidMinter;
    private CdmToDestMapper cdmToDestMapper = new CdmToDestMapper();
    private WorkGeneratorFactory workGeneratorFactory;

    private List<DestinationSipEntry> destEntries = new ArrayList<>();

    public SipService() {
    }

    private void initDependencies(SipGenerationOptions options, Connection conn) throws IOException {
        redirectMappingService = new RedirectMappingService(project);
        redirectMappingService.init();
        sipPremisLogger = new SipPremisLogger();
        sipPremisLogger.setPremisLoggerFactory(premisLoggerFactory);
        postMigrationReportService = new PostMigrationReportService();
        postMigrationReportService.setDescriptionsService(descriptionsService);
        postMigrationReportService.setProject(project);
        postMigrationReportService.setChompbConfig(chompbConfig);
        postMigrationReportService.init();

        workGeneratorFactory = new WorkGeneratorFactory();
        workGeneratorFactory.setOptions(options);
        workGeneratorFactory.setSourceFilesInfo(sourceFileService.loadMappings());
        workGeneratorFactory.setCdmToDestMapper(cdmToDestMapper);
        workGeneratorFactory.setSipPremisLogger(sipPremisLogger);
        workGeneratorFactory.setDescriptionsService(descriptionsService);
        workGeneratorFactory.setAccessFileService(accessFileService);
        workGeneratorFactory.setRedirectMappingService(redirectMappingService);
        workGeneratorFactory.setPidMinter(pidMinter);
        workGeneratorFactory.setPostMigrationReportService(postMigrationReportService);
        workGeneratorFactory.setAggregateTopMappingService(aggregateTopMappingService);
        workGeneratorFactory.setAggregateBottomMappingService(aggregateBottomMappingService);
        workGeneratorFactory.setStreamingMetadataService(streamingMetadataService);
        try {
            workGeneratorFactory.setPermissionsInfo(permissionsService.loadMappings(project));
        } catch (NoSuchFileException e) {
            log.debug("No permissions mappings file, no permissions will be added to the SIP");
        }
        try {
            workGeneratorFactory.setAccessFilesInfo(accessFileService.loadMappings());
        } catch (NoSuchFileException e) {
            log.debug("No access mappings file, no access files will be added to the SIP");
        }
        workGeneratorFactory.setConn(conn);
        initializeDestinations(options);
    }

    /**
     * Generate SIPS for each destination mapped in this project
     * @return list of sip objects generated
     */
    public List<MigrationSip> generateSips(SipGenerationOptions options) {
        validateProjectState();

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            initDependencies(options, conn);

            Statement stmt = conn.createStatement();

            // set up work generator progress bar
            long workCount = 0;
            var total = calculateTotalWorks(stmt);
            System.out.println("Work Generation Progress:");
            DisplayProgressUtil.displayProgress(workCount, total);

            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID + "," + CdmFieldInfo.CDM_CREATED
                        + "," + CdmIndexService.ENTRY_TYPE_FIELD
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + CdmIndexService.PARENT_ID_FIELD + " is null");
            while (rs.next()) {
                String cdmId = rs.getString(1);
                String cdmCreated = rs.getString(2) + "T00:00:00.000Z";
                String entryType = rs.getString(3);

                WorkGenerator workGen = workGeneratorFactory.create(cdmId, cdmCreated, entryType);
                // update progress bar
                workCount++;
                DisplayProgressUtil.displayProgress(workCount, total);
                try {
                    workGen.generate();
                } catch (SkipObjectException e) {
                    // Skipping
                }
            }
            DisplayProgressUtil.finishProgress();

            // set up sips progress bar
            long destinationCount = 0;
            var destinationTotal = destEntries.size();
            System.out.println("Writing SIPs:");
            DisplayProgressUtil.displayProgress(destinationCount, destinationTotal);

            // Finalize all the SIPs by closing and exporting their models
            List<MigrationSip> sips = new ArrayList<>();
            for (DestinationSipEntry entry : destEntries) {
                entry.commitModel();
                exportDepositModel(entry);
                MigrationSip sip = new MigrationSip(entry);
                sips.add(sip);
                // update progress bar
                destinationCount++;
                DisplayProgressUtil.displayProgress(destinationCount, destinationTotal);
                // Serialize the SIP info out to file
                SIP_INFO_WRITER.writeValue(sip.getSipPath().resolve(SIP_INFO_NAME).toFile(), sip);
                // Cleanup the TDB directory not that it has been exported
                try {
                    FileUtils.deleteDirectory(entry.getTdbPath().toFile());
                } catch (IOException e) {
                    log.warn("Failed to cleanup TDB directory", e);
                }
            }
            DisplayProgressUtil.finishProgress();
            redirectMappingService.addCollectionRow(options, sips);
            project.getProjectProperties().setSipsGeneratedDate(Instant.now());
            ProjectPropertiesSerialization.write(project);

            return sips;
        } catch (SQLException | IOException e) {
            throw new MigrationException("Failed to generate SIP", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
            destEntries.stream().forEach(DestinationSipEntry::close);
            redirectMappingService.closeCsv();
            postMigrationReportService.closeCsv();
        }
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
                        sipPremisLogger.addPremisEvent(entry, entry.getNewCollectionPid(), options);
                    }
                    destEntries.add(entry);
                    return entry;
                });

                if (mapping.getId().contains(":")) {
                    for (String cdmId : listCdmIdsByArchivalCollectionId(mapping.getId())) {
                        cdmToDestMapper.put(cdmId, destEntry);
                    }
                } else {
                    cdmToDestMapper.put(mapping.getId(), destEntry);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to load destination mappings", e);
        }
    }

    /**
     * @param id archival collection id
     * @return list of CDM ids
     */
    private List<String> listCdmIdsByArchivalCollectionId(String id) {
        String[] splitId = id.split(":");
        String idField = splitId[0];
        String idValue = splitId[1];

        List<String> cdmIds = new ArrayList<>();
        if (idValue.isEmpty()) {
            return cdmIds;
        }

        Connection conn = null;
        try {
            conn = indexService.openDbConnection();
            Statement stmt = conn.createStatement();
            // skip over values from children of compound objects, since they must go to the same destination as their parent work
            ResultSet rs = stmt.executeQuery("select " + CdmFieldInfo.CDM_ID
                    + " from " + CdmIndexService.TB_NAME
                    + " where " + " ("+ CdmIndexService.ENTRY_TYPE_FIELD + " != '"
                    + CdmIndexService.ENTRY_TYPE_COMPOUND_CHILD + "'" +
                    " OR " + CdmIndexService.ENTRY_TYPE_FIELD + " is null)" +
                    " AND " + idField + " = '" + idValue + "'");
            while (rs.next()) {
                cdmIds.add(rs.getString(1));
            }
            return cdmIds;
        } catch (SQLException e) {
            throw new MigrationException("Error interacting with export index", e);
        } finally {
            CdmIndexService.closeDbConnection(conn);
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

    /**
     * @return Count of works for progress bar
     * @throws SQLException
     */
    private long calculateTotalWorks(Statement statement) throws SQLException {
        var count = statement.executeQuery("select COUNT(*) from " + CdmIndexService.TB_NAME
                + " where " + CdmIndexService.PARENT_ID_FIELD + " is null");
        return count.getInt(1);
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

    public void setPermissionsService(PermissionsService permissionsService) {
        this.permissionsService = permissionsService;
    }

    public void setStreamingMetadataService(StreamingMetadataService streamingMetadataService) {
        this.streamingMetadataService = streamingMetadataService;
    }

    public void setPidMinter(PIDMinter pidMinter) {
        this.pidMinter = pidMinter;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }

    public void setChompbConfig(ChompbConfigService.ChompbConfig chompbConfig) {
        this.chompbConfig = chompbConfig;
    }

    public void setAggregateTopMappingService(AggregateFileMappingService aggregateTopMappingService) {
        this.aggregateTopMappingService = aggregateTopMappingService;
    }

    public void setAggregateBottomMappingService(AggregateFileMappingService aggregateBottomMappingService) {
        this.aggregateBottomMappingService = aggregateBottomMappingService;
    }

    public static class SkipObjectException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
