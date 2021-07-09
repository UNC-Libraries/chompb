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

import picocli.CommandLine.Option;

/**
 * Destination mapping generation options
 * @author bbpennel
 */
public class GenerateDestinationMappingOptions {

    @Option(names = {"-dd", "--default-dest"},
            description = {
                    "Default box-c deposit destination for objects in this migration project.",
                    "The default will be used during SIP generation for objects if they do not have explicit mappings.",
                    "Must be a UUID or other valid PID format."})
    private String defaultDestination;

    @Option(names = {"-dc", "--default-coll"},
            description = {
                    "Default collection id for objects in this migration project.",
                    "The default will be used during SIP generation for objects if they do not have explicit mappings.",
                    "If this is populated, a new collection will be created and used "
                            + "as the destination for objects mapped to the default destination.",
                    "Can be any value that uniquely identifies objects that belong in this destiantion.",})
    private String defaultCollection;

    @Option(names = { "-f", "--force"},
            description = "Overwrite destination mapping if one already exists")
    private boolean force;

    public String getDefaultDestination() {
        return defaultDestination;
    }

    public void setDefaultDestination(String defaultDestination) {
        this.defaultDestination = defaultDestination;
    }

    public String getDefaultCollection() {
        return defaultCollection;
    }

    public void setDefaultCollection(String defaultCollection) {
        this.defaultCollection = defaultCollection;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
