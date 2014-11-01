package com.petpal.tracking.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by per on 10/28/14.
 */
public class JSONUtil {

    public static String convertToString(Object o) {

        ObjectMapper mapper = new ObjectMapper();
        String json = null;

        try {
            json = mapper.writeValueAsString(o);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return json;
    }
}
