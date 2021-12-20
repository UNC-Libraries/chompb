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
package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine;

/**
 * Options for CDM export operation
 *
 * @author bbpennel
 */
public class CdmExportOptions {
    @CommandLine.Option(names = { "--cdm-url" },
            description = {"Base URL to the CDM web service API. Falls back to CDM_BASE_URL env variable.",
                    "Default: ${DEFAULT-VALUE}"},
            defaultValue = "${env:CDM_BASE_URL:-http://localhost:82/}")
    private String cdmBaseUri;
    @CommandLine.Option(names = { "-u", "--cdm-user"},
            description = {"User name for CDM requests.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String cdmUsername;
    @CommandLine.Option(names = {"-n", "-per-page"},
            description = {"Page size for exports.",
                    "Default: ${DEFAULT-VALUE}. Max page size is 5000"},
            defaultValue = "1000")
    private int pageSize;

    public String getCdmBaseUri() {
        return cdmBaseUri;
    }

    public void setCdmBaseUri(String cdmBaseUri) {
        this.cdmBaseUri = cdmBaseUri;
    }

    public String getCdmUsername() {
        return cdmUsername;
    }

    public void setCdmUsername(String cdmUsername) {
        this.cdmUsername = cdmUsername;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
