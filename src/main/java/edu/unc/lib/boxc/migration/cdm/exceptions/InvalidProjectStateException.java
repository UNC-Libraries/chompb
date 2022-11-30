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
