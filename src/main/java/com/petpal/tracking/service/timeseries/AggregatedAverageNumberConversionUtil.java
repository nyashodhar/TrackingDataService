package com.petpal.tracking.service.timeseries;

import org.springframework.util.Assert;

import java.text.DecimalFormat;

/**
 * Scheme for storing an aggregated average number that is continuously updated in a string:
 *
 * Serialization:
 *
 *      "s: XXXXX.YYY w: ZZZZ"
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
 * Created by per on 12/4/14.
 */
public class AggregatedAverageNumberConversionUtil {

    private static final String TOKEN_SUM = "s: ";
    private static final String TOKEN_WEIGHTS = "w: ";

    private static final String ERROR_SUM_MISSING = "Sum not specified.";
    private static final String ERROR_WEIGHTS_MISSING = "Weights not specified.";
    private static final String ERROR_AVERAGE_NUMBER_MISSING = "Average number not specified.";
    private static final String ERROR_SERIALIZED_AVERAGE_MISSING = "Existing serialized average not specified.";
    private static final String PARSE_ERROR_WEIGHTS_TOKEN_MISSING = "Weights token not present in string";
    private static final String PARSE_ERROR_SUM_TOKEN_MISSING = "Sum token not present in string";
    private static final String PARSE_ERROR_SUM_TOKEN_NOT_START = "The sum token is present, but not at the start of the string";
    private static final String PARSE_ERROR_MISSING_SUM = "Sum not present in string";
    private static final String PARSE_ERROR_MISSING_WEIGHTS = "Weights not present in string";
    private static final String PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER = "Weights not a positive number";

    private static final DecimalFormat decimalFormat;

    static {
        decimalFormat = new DecimalFormat("#");
        decimalFormat.setMinimumFractionDigits(3);
        decimalFormat.setMaximumFractionDigits(3);
        decimalFormat.setMinimumIntegerDigits(1);
    }

    public static String createNew(Double averageNumber) {
        Assert.notNull(averageNumber, ERROR_AVERAGE_NUMBER_MISSING);
        return createSerializedAverage(averageNumber, 1);
    }

    public static String updateSerializedAverage(Double averageNumber, String existingSerializedAverage) {

        Assert.notNull(averageNumber, ERROR_SUM_MISSING);
        Assert.notNull(existingSerializedAverage, ERROR_SERIALIZED_AVERAGE_MISSING);

        try {
            double existingSum = extractSumFromSerializedAverage(existingSerializedAverage);
            int existingWeights = extractWeightsFromSerializedAverage(existingSerializedAverage);
            return createSerializedAverage(existingSum + averageNumber, existingWeights + 1);
        } catch(Throwable t) {
            throw new IllegalArgumentException("Unable to add value " + averageNumber +
                    " to existing serialized average " + existingSerializedAverage, t);
        }
    }

    public static double getAverageFromSerializedAverage(String serializedAverage) {
        Assert.notNull(serializedAverage, ERROR_SERIALIZED_AVERAGE_MISSING);
        try {
            double sum = extractSumFromSerializedAverage(serializedAverage);
            int weights = extractWeightsFromSerializedAverage(serializedAverage);
            double avg = sum / ((double) weights);
            return Double.parseDouble(decimalFormat.format(avg));
        } catch(Throwable t) {
            throw new IllegalArgumentException("Unable to get average value " +
                    "from serialized average " + serializedAverage, t);
        }
    }

    protected static String createSerializedAverage(Double sum, Integer weights) {

        Assert.notNull(sum, ERROR_SUM_MISSING);
        Assert.notNull(weights, ERROR_WEIGHTS_MISSING);
        Assert.isTrue(weights.intValue() > 0, PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(TOKEN_SUM);
        stringBuilder.append(decimalFormat.format(sum));
        stringBuilder.append(" ");
        stringBuilder.append(TOKEN_WEIGHTS);
        stringBuilder.append(weights);

        return stringBuilder.toString();
    }

    protected static double extractSumFromSerializedAverage(String existingSerializedAverage) {

        int sumTokenStart = existingSerializedAverage.indexOf(TOKEN_SUM);
        int weightsTokenStart = existingSerializedAverage.indexOf(TOKEN_WEIGHTS);

        Assert.isTrue(sumTokenStart != -1, PARSE_ERROR_SUM_TOKEN_MISSING);
        Assert.isTrue(sumTokenStart == 0, PARSE_ERROR_SUM_TOKEN_NOT_START);
        Assert.isTrue(weightsTokenStart != -1, PARSE_ERROR_WEIGHTS_TOKEN_MISSING);
        Assert.isTrue(weightsTokenStart > TOKEN_SUM.length(), PARSE_ERROR_MISSING_SUM);

        String sumStr = existingSerializedAverage.substring(
                TOKEN_SUM.length(), existingSerializedAverage.indexOf(TOKEN_WEIGHTS)).trim();
        return Double.parseDouble(sumStr);
    }

    protected static int extractWeightsFromSerializedAverage(String existingSerializedAverage) {

        int weightsTokenStart = existingSerializedAverage.indexOf(TOKEN_WEIGHTS);
        Assert.isTrue(weightsTokenStart != -1, PARSE_ERROR_WEIGHTS_TOKEN_MISSING);
        Assert.isTrue(weightsTokenStart + TOKEN_WEIGHTS.length() < existingSerializedAverage.length(), PARSE_ERROR_MISSING_WEIGHTS);

        String weightStr = existingSerializedAverage.substring(
                weightsTokenStart + TOKEN_WEIGHTS.length(), existingSerializedAverage.length()).trim();

        int weights = Integer.parseInt(weightStr);

        Assert.isTrue(weights > 0, PARSE_ERROR_WEIGHTS_NOT_POSITIVE_NUMBER);

        return Integer.parseInt(weightStr);
    }
}
