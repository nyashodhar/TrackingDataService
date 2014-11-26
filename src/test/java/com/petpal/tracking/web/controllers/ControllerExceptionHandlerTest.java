package com.petpal.tracking.web.controllers;

import com.petpal.tracking.web.errors.InvalidControllerArgumentException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by per on 11/25/14.
 */
public class ControllerExceptionHandlerTest {

    // Class under test
    private ControllerExceptionHandler controllerExceptionHandler;

    @Before
    public void setup() {
        controllerExceptionHandler = new ControllerExceptionHandler();
    }

    @Test
    public void testHandleInvalidControllerArgumentException() {

        InvalidControllerArgumentException cause = new InvalidControllerArgumentException("hello");

        ResponseEntity<Map<String, Serializable>> responseEntity =
                controllerExceptionHandler.handleInvalidControllerArgumentException(cause);

        Assert.assertEquals(responseEntity.getStatusCode(), HttpStatus.BAD_REQUEST);

        Map<String, Serializable> responseBody = responseEntity.getBody();

        Assert.assertEquals(responseEntity.getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(responseBody.get("status").toString(), Integer.toString(HttpStatus.BAD_REQUEST.value()));
        Assert.assertEquals(responseBody.get("error").toString(), cause.getMessage());

        // Ensure the timestamp is very close to 'now'
        long timestampInResponse = Long.parseLong(responseBody.get("timestamp").toString());
        Assert.assertTrue(System.currentTimeMillis()-3000L < timestampInResponse);
        Assert.assertTrue(System.currentTimeMillis()+3000L > timestampInResponse);
    }
}
