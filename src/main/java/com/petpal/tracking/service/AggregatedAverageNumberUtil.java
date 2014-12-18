package com.petpal.tracking.service;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.Assert;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Scheme for storing an aggregated average number that is continuously
 * updated and rolled up into a string:
 *
 * Serialization:
 *
 *      "{"s":"XXXXX.YYY","w":"ZZZZ"}"
 *
 * XXXXX.YYY is a string formatted double representing a sum
 * ZZZZ is an integer representing the number of weights
 *
 * Deserialization:
 *
 *     Average = XXXXX.YYY/ZZZZ
 *
 * Updating the value:
 *
 *     Update the sum
 *     Increment the weights
 *
 * Created by per on 12/16/14.
 */
public class AggregatedAverageNumberUtil {

    private static String KEY_SUM = "s";
    private static String KEY_WEIGHT = "w";

    private static final String ERROR_SUM_MISSING = "Sum not specified.";
    private static final String ERROR_WEIGHTS_MISSING = "Weights not specified.";
    private static final String ERROR_AVERAGE_NUMBER_MISSING = "Average number not specified.";
    private static final String ERROR_SERIALIZED_AVERAGE_MISSING = "Existing serialized average not specified.";
    private static final String PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER = "Weights not a positive number";

    private static final DecimalFormat decimalFormat;

    private static final ObjectMapper objectMapper;

    static {
        decimalFormat = new DecimalFormat("#");
        decimalFormat.setMinimumFractionDigits(3);
        decimalFormat.setMaximumFractionDigits(3);
        decimalFormat.setMinimumIntegerDigits(1);
        objectMapper = new ObjectMapper();
    }

    public String createNew(Double averageNumber) {
        Assert.notNull(averageNumber, ERROR_AVERAGE_NUMBER_MISSING);
        return createSerializedAverage(averageNumber, 1);
    }

    protected String createSerializedAverage(Double sum, Integer weights) {

        Assert.notNull(sum, ERROR_SUM_MISSING);
        Assert.notNull(weights, ERROR_WEIGHTS_MISSING);
        Assert.isTrue(weights.intValue() > 0, PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER);

        Map<String, String> serializedAvg = new HashMap<String, String>();
        serializedAvg.put(KEY_SUM, decimalFormat.format(sum));
        serializedAvg.put(KEY_WEIGHT, Integer.toString(weights.intValue()));

        try {
            return objectMapper.writeValueAsString(serializedAvg);
        } catch (JsonGenerationException e) {
            throw new RuntimeException("Error when creating json from object " + serializedAvg, e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Error when creating json from object " + serializedAvg, e);
        } catch (IOException e) {
            throw new RuntimeException("Error when creating json from object " + serializedAvg, e);
        }
    }

    public String updateSerializedAverage(Double averageNumber, String existingSerializedAverage) {

        Assert.notNull(averageNumber, ERROR_SUM_MISSING);
        Assert.notNull(existingSerializedAverage, ERROR_SERIALIZED_AVERAGE_MISSING);

        Map<String, String> deserializedAvgMap =
                deserializeExistingValue(existingSerializedAverage);

        Double existingSum = Double.parseDouble(deserializedAvgMap.get(KEY_SUM));
        Integer existingWeights = Integer.parseInt(deserializedAvgMap.get(KEY_WEIGHT));
        Assert.isTrue(existingWeights.intValue() > 0, PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER);

        try {
            return createSerializedAverage(existingSum + averageNumber, existingWeights + 1);
        } catch(Throwable t) {
            throw new IllegalArgumentException("Unable to add value " + averageNumber +
                    " to existing serialized average " + existingSerializedAverage, t);
        }
    }

    public double getAverageFromSerializedAverage(String serializedAverage) {
        Assert.notNull(serializedAverage, ERROR_SERIALIZED_AVERAGE_MISSING);

        Map<String, String> deserializedAvgMap =
                deserializeExistingValue(serializedAverage);

        try {
            Double sum = Double.parseDouble(deserializedAvgMap.get(KEY_SUM));
            Integer weights = Integer.parseInt(deserializedAvgMap.get(KEY_WEIGHT));
            double avg = sum.doubleValue() / weights.doubleValue();
            return Double.parseDouble(decimalFormat.format(avg));
        } catch(Throwable t) {
            throw new IllegalArgumentException("Unable to get average value " +
                    "from serialized average " + serializedAverage, t);
        }
    }


    protected Map<String, String> deserializeExistingValue(String json) {

        Map<String, String> deserializedAvgMap;

        try {
            deserializedAvgMap = objectMapper.readValue(json, Map.class);
        } catch(JsonParseException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        } catch(JsonMappingException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        } catch(IOException e) {
            throw new RuntimeException("Error when creating map from json " + json, e);
        }

        return deserializedAvgMap;
    }
}
