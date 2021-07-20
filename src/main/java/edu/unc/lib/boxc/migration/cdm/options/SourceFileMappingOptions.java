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

import java.nio.file.Path;

import picocli.CommandLine.Option;

/**
 * Options for generating source file mappings
 *
 * @author bbpennel
 */
public class SourceFileMappingOptions {

    @Option(names = {"-b", "--base-path"},
            description = {
                    "Required. Base file path to search in for source files to match with.",
                    "By default, all files which are immediate children of base path will be used.",
                    "To change this behavior, use the -g option."})
    private Path basePath;

    @Option(names = {"-g", "--glob-pattern"},
            description = {
                    "Optional pattern for adjusting which files within the base path to search.",
                    "Must be relative to the base path. Follows glob pattern syntax.",
                    "For example, to only match tiff files within the base path:",
                    "    *.tiff",
                    "To match any file within any directory at a depth of 2 subdirectories:",
                    "    */*/*",
                    "Or to match tiff files at any depth:",
                    "    **/*.tiff"})
    private String pathPattern;

    @Option(names = {"-n", "--field-name"},
            description = {
                    "Name of the CDM export field which will be transformed by the field matching pattern "
                    + "to produce the source file filename for matching purposes."},
            defaultValue = "file")
    private String exportField;

    @Option(names = {"-p", "--field-pattern"},
            description = {
                    "Regular expression which will be used to extract portions of the export field value "
                    + "for use in the filename template. Use matching groups for this.",
                    "Must match the entire value of the export field.",
                    "For example, to extract numeric portions of the value: 276_214_E.tif",
                    "You could provide the pattern: (\\d+)\\_(\\d+)_E.tif"},
            defaultValue = "(.+)")
    private String fieldMatchingPattern;

    @Option(names = {"-t", "--file-template"},
            description = {
                    "Template used to produce expected source file filenames.",
                    "It should be used with matching groups from --field-pattern.",
                    "NOTE: Use single quotes to wrap this value, or escape the $ characters as \\$.",
                    "Given the field pattern above, it could be templated out to: 00276_op0214_0001_e.tif",
                    "With the template: 00$1_op0$2_0001_e.tif"},
            defaultValue = "$1")
    private String filenameTemplate;

    @Option(names = {"-l", "--lower-template"},
            description = "Convert the filename produced from the --file-temp option to lowercase "
                    + "prior to attempting to match against source files.")
    private boolean lowercaseTemplate;

    @Option(names = {"-u", "--update"},
            description = {
                    "If provided, then any source file matches produced will be used to update an existing"
                    + " source file mapping file, instead of attempting to create a new one.",
                    "This can be used to build up the mapping in multiple passes"})
    private boolean update;

    @Option(names = {"-d", "--dry-run"},
            description = {
                    "If provided, then the output of the matching will be displayed in the console rather "
                    + "than written to file"})
    private boolean dryRun;

    @Option(names = { "-f", "--force"},
            description = "Overwrite mapping file if one already exists")
    private boolean force;

    public Path getBasePath() {
        return basePath;
    }

    public void setBasePath(Path basePath) {
        this.basePath = basePath;
    }

    public String getPathPattern() {
        return pathPattern;
    }

    public void setPathPattern(String pathPattern) {
        this.pathPattern = pathPattern;
    }

    public String getExportField() {
        return exportField;
    }

    public void setExportField(String exportField) {
        this.exportField = exportField;
    }

    public String getFieldMatchingPattern() {
        return fieldMatchingPattern;
    }

    public void setFieldMatchingPattern(String fieldMatchingPattern) {
        this.fieldMatchingPattern = fieldMatchingPattern;
    }

    public String getFilenameTemplate() {
        return filenameTemplate;
    }

    public void setFilenameTemplate(String filenameTemplate) {
        this.filenameTemplate = filenameTemplate;
    }

    public boolean isLowercaseTemplate() {
        return lowercaseTemplate;
    }

    public void setLowercaseTemplate(boolean lowercaseTemplate) {
        this.lowercaseTemplate = lowercaseTemplate;
    }

    public boolean getUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
