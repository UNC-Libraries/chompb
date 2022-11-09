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
package edu.unc.lib.boxc.migration.cdm.util;

import org.apache.commons.csv.CSVFormat;

/**
 * Constants related to the post migration report
 *
 * @author bbpennel
 */
public class PostMigrationReportConstants {
    public static final String BXC_URL_HEADER = "boxc_url";
    public static final String VERIFIED_HEADER = "verified";
    public static final int VERIFIED_INDEX = 5;
    public static final String[] CSV_HEADERS = new String[] {
            "cdm_id", "cdm_url", "boxc_obj_type", "boxc_url", "boxc_title", VERIFIED_HEADER,
            "boxc_parent_work_url", "boxc_parent_work_title", "children_count" };
    public static final CSVFormat CSV_OUTPUT_FORMAT = CSVFormat.Builder.create()
            .setHeader(CSV_HEADERS)
            .build();
    public static final CSVFormat CSV_PARSER_FORMAT = CSVFormat.Builder.create()
                .setSkipHeaderRecord(true)
                .setHeader(CSV_HEADERS)
                .setTrim(true)
                .build();

    private PostMigrationReportConstants() {
    }
}
