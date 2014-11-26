package com.petpal.tracking.web.controllers;

import org.springframework.web.bind.annotation.ControllerAdvice;

/**
 * Created by per on 10/28/14.
 */

@ControllerAdvice
public class ControllerExceptionHandler {

    /*
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleIllegalArgumentException(IllegalArgumentException e) {
        Map<String, String> errors = new HashMap<String, String>();
        errors.put("error", e.getMessage());
        return new ResponseEntity(errors, HttpStatus.BAD_REQUEST);
    }
    */
}
