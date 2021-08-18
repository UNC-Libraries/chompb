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
package edu.unc.lib.boxc.migration.cdm.status;

import static edu.unc.lib.boxc.migration.cdm.util.CLIConstants.outputLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;
import edu.unc.lib.boxc.migration.cdm.services.CdmIndexService;

/**
 * @author bbpennel
 */
public class AbstractStatusService {
    protected static final String INDENT = "    ";
    private static final int MIN_LABEL_WIDTH = 20;

    private CdmIndexService indexService;
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

    protected int countIndexedObjects() {
        CdmIndexService indexService = new CdmIndexService();
        indexService.setProject(project);
        try (Connection conn = indexService.openDbConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + CdmIndexService.TB_NAME);
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new MigrationException("Failed to determine number of objects", e);
        }
    }

    protected CdmIndexService getIndexService() {
        if (indexService == null) {
            indexService = new CdmIndexService();
            indexService.setProject(project);
        }
        return indexService;
    }

    public void setProject(MigrationProject project) {
        this.project = project;
    }
}
