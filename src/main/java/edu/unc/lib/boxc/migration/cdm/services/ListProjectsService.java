package edu.unc.lib.boxc.migration.cdm.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.jobs.VelocicroptorRemoteJob;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Service for listing all chompb projects in a given directory
 * @author krwong
 */
public class ListProjectsService {
    private static final Logger log = LoggerFactory.getLogger(ListProjectsService.class);

    private CdmFieldService fieldService;
    private CdmIndexService indexService;
    private ProjectPropertiesService propertiesService;
    private SourceFileService sourceFileService;

    public static final String PROJECT_PATH = "projectPath";
    public static final String STATUS = "status";
    public static final String ALLOWED_ACTIONS = "allowedActions";
    public static final String PROJECT_PROPERTIES = "projectProperties";
    public static final String PROCESSING_JOBS = "processingJobs";
    public static final String PENDING = "pending";
    public static final String COMPLETED = "completed";
    private static final Set<String> IMAGE_FORMATS = new HashSet<>(Arrays.asList("tif", "tiff", "jpeg", "jpg", "png",
            "gif", "pict", "bmp", "psd", "jp2", "nef", "crw", "cr2", "dng", "raf"));

    /**
     * List projects in given directory
     * @return jsonNode of projects
     */
    public JsonNode listProjects(Path directory, boolean includeArchived) throws Exception {
        if (Files.notExists(directory)) {
            throw new InvalidProjectStateException("Path " + directory + " does not exist");
        }

        // create JSON
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        ArrayNode arrayNode = mapper.createArrayNode();

        // list projects
        listProjectsInDirectory(directory, mapper, arrayNode);

        // list archived projects
        if (includeArchived) {
            listProjectsInDirectory(directory.resolve(ArchiveProjectsService.ARCHIVED), mapper, arrayNode);
        }

        log.debug(arrayNode.toString());
        return arrayNode;
    }

    private void listProjectsInDirectory(Path directory, ObjectMapper mapper, ArrayNode arrayNode) throws Exception {
        for (File file : directory.toFile().listFiles()) {
            if (file.isDirectory()) {
                try {
                    MigrationProject project = initializeProject(file.toPath());

                    Path projectPath = file.toPath().toAbsolutePath();
                    String projectStatus = status(project);
                    ArrayNode allowedActions = mapper.createArrayNode();
                    if (!projectStatus.equals("archived")) {
                        allowedActions = mapper.valueToTree(allowedActions(project));
                    }
                    JsonNode projectProperties = mapper.readTree(project.getProjectPropertiesPath().toFile());
                    ObjectNode processingJobs = processingJobs(project, mapper);

                    // add project info to JSON
                    ObjectNode objectNode = mapper.createObjectNode();
                    objectNode.put(PROJECT_PATH, projectPath.toString());
                    objectNode.put(STATUS, projectStatus);
                    objectNode.putArray(ALLOWED_ACTIONS).addAll(allowedActions);
                    objectNode.set(PROJECT_PROPERTIES, projectProperties);
                    objectNode.set(PROCESSING_JOBS, processingJobs);
                    arrayNode.add(objectNode);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
    }

    /**
     * Condensed representation of the phase a project is in
     * possible values: initialized, indexed, sources_mapped, sips_generated, ingested, archived
     * @return project status
     */
    private String status(MigrationProject project) {
        String status = null;

        if (project.getProjectPath().toString().contains("/" + ArchiveProjectsService.ARCHIVED + "/")) {
            status = "archived";
        } else if (!project.getProjectProperties().getSipsSubmitted().isEmpty()) {
            status = "ingested";
        } else if (project.getProjectProperties().getSipsGeneratedDate() != null) {
            status = "sips_generated";
        } else if (project.getProjectProperties().getSourceFilesUpdatedDate() != null) {
            status = "sources_mapped";
        } else if (project.getProjectProperties().getIndexedDate() != null) {
            status = "indexed";
        } else if (Files.exists(project.getProjectPropertiesPath())) {
            status = "initialized";
        }

        return status;
    }

    /**
     * Tell other applications (boxc) what processes are supported by the project
     * crop_color_bars: populated if source files are mapped and if project contains any images (based off source file extensions)
     * @return allowed_actions
     */
    private List<String> allowedActions(MigrationProject project) throws Exception {
        List<String> allowedActions = new ArrayList<>();

        if (project.getProjectProperties().getSourceFilesUpdatedDate() != null) {
            SourceFilesInfo info = sourceFileService.loadMappings();
            if (info.getMappings().stream().map(entry ->
                    FilenameUtils.getExtension(entry.getFirstSourcePath().toString().toLowerCase()))
                    .anyMatch(IMAGE_FORMATS::contains)) {
                allowedActions.add("crop_color_bars");
            }
        }
        return allowedActions;
    }

    public ObjectNode processingJobs(MigrationProject project, ObjectMapper mapper) throws Exception {
        ObjectNode processingJobs = mapper.createObjectNode();

        // velocicropter report status
        ObjectNode velocicropterStatus = mapper.createObjectNode();
        if (Files.exists(project.getProjectPath().resolve(VelocicroptorRemoteJob.RESULTS_REL_PATH
                + "/job_completed"))) {
            velocicropterStatus.put(STATUS, COMPLETED);
            processingJobs.set(VelocicroptorRemoteJob.JOB_NAME, velocicropterStatus);
        } else if (Files.exists(project.getProjectPath().resolve(VelocicroptorRemoteJob.RESULTS_REL_PATH))) {
            velocicropterStatus.put(STATUS, PENDING);
            processingJobs.set(VelocicroptorRemoteJob.JOB_NAME, velocicropterStatus);
        }

        return processingJobs;
    }

    public MigrationProject initializeProject(Path path) throws Exception {
        MigrationProject project = MigrationProjectFactory.loadMigrationProject(path);
        propertiesService.setProject(project);
        sourceFileService.setProject(project);
        indexService.setProject(project);
        fieldService.setProject(project);

        return project;
    }

    public void setFieldService(CdmFieldService fieldService) {
        this.fieldService = fieldService;
    }

    public void setIndexService(CdmIndexService indexService) {
        this.indexService = indexService;
    }

    public void setPropertiesService(ProjectPropertiesService propertiesService) {
        this.propertiesService = propertiesService;
    }

    public void setSourceFileService(SourceFileService sourceFileService) {
        this.sourceFileService = sourceFileService;
    }
}
