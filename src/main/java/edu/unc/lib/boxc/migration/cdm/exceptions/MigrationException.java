package edu.unc.lib.boxc.migration.cdm.exceptions;

/**
 * @author bbpennel
 */
public class MigrationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MigrationException() {
    }

    public MigrationException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public MigrationException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public MigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
