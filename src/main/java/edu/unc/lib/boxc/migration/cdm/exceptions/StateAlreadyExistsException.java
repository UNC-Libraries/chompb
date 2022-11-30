package edu.unc.lib.boxc.migration.cdm.exceptions;

/**
 * Exception thrown when a project already contains files or state information
 * @author bbpennel
 */
public class StateAlreadyExistsException extends MigrationException {
    private static final long serialVersionUID = 1L;

    /**
     * @param message
     */
    public StateAlreadyExistsException(String message) {
        super(message);
    }
}
