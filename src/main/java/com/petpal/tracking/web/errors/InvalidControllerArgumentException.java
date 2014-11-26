package com.petpal.tracking.web.errors;

/**
 * Created by per on 11/25/14.
 */
public class InvalidControllerArgumentException extends RuntimeException {

    public InvalidControllerArgumentException(String message) {
        super(message);
    }

}
