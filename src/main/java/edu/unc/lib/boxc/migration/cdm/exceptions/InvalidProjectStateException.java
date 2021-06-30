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
package edu.unc.lib.boxc.migration.cdm.exceptions;

/**
 * Exception indicating that a MigrationProject is in an invalid state
 *
 * @author bbpennel
 */
public class InvalidProjectStateException extends MigrationException {
    private static final long serialVersionUID = 1L;

    /**
     */
    public InvalidProjectStateException() {
    }

    /**
     * @param message
     */
    public InvalidProjectStateException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public InvalidProjectStateException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public InvalidProjectStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
