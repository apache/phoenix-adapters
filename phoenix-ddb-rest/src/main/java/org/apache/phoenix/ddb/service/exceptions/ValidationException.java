package org.apache.phoenix.ddb.service.exceptions;

/**
 * This exception is thrown when API requests fails to satisfy certain constraints.
 */
public class ValidationException extends RuntimeException {
    public ValidationException(String message) {super(message);}
}
