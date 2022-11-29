package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Iterators;

import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * @author bbpennel
 */
public class AbstractStatusService {
    protected static final String INDENT = "    ";
    private static final int MIN_LABEL_WIDTH = 20;

    private StatusQueryService queryService;
    protected MigrationProject project;

    protected void showField(String label, Object value) {
        int padding = MIN_LABEL_WIDTH - label.length();
        outputLogger.info("{}{}: {}{}", INDENT, label, StringUtils.repeat(' ', padding), value);
    }

    protected void showFieldWithPercent(String label, int value, int total) {
        int padding = MIN_LABEL_WIDTH - label.length();
        double percent = (double) value / total * 100;
        outputLogger.info("{}{}: {}{} ({}%)", INDENT, label, StringUtils.repeat(' ', padding),
                value, String.format("%.1f", percent));
    }

    protected void showFieldListValues(Collection<String> values) {
        for (String value : values) {
            outputLogger.info("{}{}* {}", INDENT, INDENT, value);
        }
    }

    protected void sectionDivider() {
        outputLogger.info("");
    }

    public StatusQueryService getQueryService() {
        if (queryService == null) {
            return initializeQueryService();
        }
        return queryService;
    }

    public void setQueryService(StatusQueryService queryService) {
        this.queryService = queryService;
    }

    public StatusQueryService initializeQueryService() {
        this.queryService = new StatusQueryService(project);
        return this.queryService;
    }

    protected int countXmlDocuments(Path dirPath) {
        try (DirectoryStream<Path> pathStream = Files.newDirectoryStream(dirPath, "*.xml")) {
            return Iterators.size(pathStream.iterator());
        } catch (FileNotFoundException e) {
            return 0;
        } catch (IOException e) {
            outputLogger.info("Unable to count files for {}", dirPath, e);
            return 0;
        }
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
