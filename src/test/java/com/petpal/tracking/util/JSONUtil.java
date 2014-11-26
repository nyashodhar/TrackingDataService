package com.petpal.tracking.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by per on 10/28/14.
 */
public class JSONUtil {

    public static String convertToString(Object o) {

        ObjectMapper mapper = new ObjectMapper();
        String json;

        try {
            json = mapper.writeValueAsString(o);
        } catch (JsonGenerationException e) {
            throw new RuntimeException("Error when creating json from object " + o, e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Error when creating json from object " + o, e);
        } catch (IOException e) {
            throw new RuntimeException("Error when creating json from object " + o, e);
        }

        return json;
    }

    public static Map<String, Serializable> jsonToMap(String json) {

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Serializable> mappedJson;

        try {
            mappedJson = objectMapper.readValue(json, Map.class);
        } catch(JsonParseException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        } catch(JsonMappingException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        } catch(IOException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        }

        return mappedJson;
    }
}
