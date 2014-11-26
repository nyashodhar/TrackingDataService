package com.petpal.tracking.web.controllers;

import com.petpal.tracking.web.errors.InvalidControllerArgumentException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by per on 10/28/14.
 */
@ControllerAdvice
public class ControllerExceptionHandler {

    /**
     * Ensure that exception thrown in custom validation does not
     * result in a 500 error, but rather a 400 error as specified
     * here.
     * @param e
     * @return
     */
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(InvalidControllerArgumentException.class)
    public ResponseEntity<Map<String, Serializable>>
        handleInvalidControllerArgumentException(InvalidControllerArgumentException e) {

        Map<String, Serializable> errors = new HashMap<String, Serializable>();
        errors.put("timestamp", System.currentTimeMillis());
        errors.put("status", HttpStatus.BAD_REQUEST.value());
        errors.put("error", e.getMessage());

        return new ResponseEntity(errors, HttpStatus.BAD_REQUEST);
    }
}
