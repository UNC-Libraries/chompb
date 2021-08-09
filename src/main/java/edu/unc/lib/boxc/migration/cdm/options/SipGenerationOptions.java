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
 * Options for sip generation operations
 * @author bbpennel
 */
public class SipGenerationOptions {

    @Option(names = { "-f", "--force"},
            description = { "Objects which are missing required details will be excluded from the SIP.",
                    "Normally the SIP generation operation would fail.",
                    "This includes missing source files and MODS descriptions."})
    private boolean force;

    @Option(names = { "-u", "--user"},
            description = {"Username of user performing the SIP, which will be recorded in PREMIS.",
                    "Defaults to current user: ${DEFAULT-VALUE}"},
            defaultValue = "${sys:user.name}")
    private String username;

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
