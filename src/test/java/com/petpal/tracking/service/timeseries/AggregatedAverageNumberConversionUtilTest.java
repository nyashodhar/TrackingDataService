package com.petpal.tracking.service.timeseries;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by per on 12/4/14.
 */
public class AggregatedAverageNumberConversionUtilTest {

    //
    // createNew()
    //

    @Test(expected = IllegalArgumentException.class)
    public void createNew_null_input() {
        AggregatedAverageNumberConversionUtil.createNew(null);
    }

    @Test
    public void createNew_null_truncated() {
        String serialized = AggregatedAverageNumberConversionUtil.createNew(1.3333);
        Assert.assertEquals("s: 1.333 w: 1", serialized);
    }

    @Test
    public void createNew_null_truncated_and_rounded() {
        String serialized = AggregatedAverageNumberConversionUtil.createNew(1.3337);
        Assert.assertEquals("s: 1.334 w: 1", serialized);
    }

    @Test
    public void createNew_null_sum_zero_padded() {
        String serialized = AggregatedAverageNumberConversionUtil.createNew(1.1);
        Assert.assertEquals("s: 1.100 w: 1", serialized);
    }

    @Test
    public void createNew_null_regular() {
        String serialized = AggregatedAverageNumberConversionUtil.createNew(0.111);
        Assert.assertEquals("s: 0.111 w: 1", serialized);
    }

    //
    // updateSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_average_number() {
        AggregatedAverageNumberConversionUtil.updateSerializedAverage(null, "s: 11.11 w: 23");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_serialized_average() {
        AggregatedAverageNumberConversionUtil.updateSerializedAverage(1.1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_number_input() {
        AggregatedAverageNumberConversionUtil.updateSerializedAverage(1.1, "s: 11.11 w: blah");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_positive_number() {
        AggregatedAverageNumberConversionUtil.updateSerializedAverage(1.1, "s: 11.11 w: 0");
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated() {
        String serialized = AggregatedAverageNumberConversionUtil.updateSerializedAverage(9.3333, "s: 1.000 w: 2");
        Assert.assertEquals("s: 10.333 w: 3", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated_and_rounded() {
        String serialized = AggregatedAverageNumberConversionUtil.updateSerializedAverage(5.6667, "s: 1.000 w: 1");
        Assert.assertEquals("s: 6.667 w: 2", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_zero_padded() {
        String serialized = AggregatedAverageNumberConversionUtil.updateSerializedAverage(5.44, "s: 1.000 w: 1");
        Assert.assertEquals("s: 6.440 w: 2", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_regular() {
        String serialized = AggregatedAverageNumberConversionUtil.updateSerializedAverage(0.111, "s: 0.111 w: 1");
        Assert.assertEquals("s: 0.222 w: 2", serialized);
    }

    //
    // getAverageFromSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_null_input() {
        AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_sum_not_a_number_input() {
        AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage("s: blah w: 11");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_weights_not_a_positive_number() {
        AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage("s: 11.1234 w: 0");
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated() {
        double avg = AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage("s: 22.2222 w: 2");
        Assert.assertEquals(11.111, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated_and_rounded() {
        double avg = AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage("s: 22.2234 w: 2");
        Assert.assertEquals(11.112, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_regular() {
        double avg = AggregatedAverageNumberConversionUtil.getAverageFromSerializedAverage("s: 0.222 w: 2");
        Assert.assertEquals(0.111, avg, 0);
    }

    //
    // createSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSerializedAverage_sum_missing() {
        AggregatedAverageNumberConversionUtil.createSerializedAverage(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSerializedAverage_weights_missing() {
        AggregatedAverageNumberConversionUtil.createSerializedAverage(1.1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSerializedAverage_weights_not_positive() {
        AggregatedAverageNumberConversionUtil.createSerializedAverage(1.1, 0);
    }

    @Test
    public void testCreateSerializedAverage_sum_truncated() {
        String serialized = AggregatedAverageNumberConversionUtil.createSerializedAverage(1.1234, 1);
        Assert.assertEquals("s: 1.123 w: 1", serialized);
    }

    @Test
    public void testCreateSerializedAverage_sum_truncated_and_rounded() {
        String serialized = AggregatedAverageNumberConversionUtil.createSerializedAverage(1.1237, 1);
        Assert.assertEquals("s: 1.124 w: 1", serialized);
    }

    @Test
    public void testCreateSerializedAverage_sum__zero_padded() {
        String serialized = AggregatedAverageNumberConversionUtil.createSerializedAverage(1.12, 1);
        Assert.assertEquals("s: 1.120 w: 1", serialized);
    }

    @Test
    public void testCreateSerializedAverage_sum_regular() {
        String serialized = AggregatedAverageNumberConversionUtil.createSerializedAverage(0.123, 1);
        Assert.assertEquals("s: 0.123 w: 1", serialized);
    }

    //
    // extractSumFromSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSumFromSerializedAverage_sum_token_missing() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSumFromSerializedAverage_sum_token_not_at_beginning() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage(" s: 123.123 w: 123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSumFromSerializedAverage_weights_token_missing() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage(" s: 123.123 123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSumFromSerializedAverage_missing_sum() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s: w: 123");
    }

    @Test(expected = NumberFormatException.class)
    public void testExtractSumFromSerializedAverage_sum_not_a_double_scenario1() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s: blah blah w: 123");
    }

    @Test(expected = NumberFormatException.class)
    public void testExtractSumFromSerializedAverage_sum_not_a_double_scenario2() {
        AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s:        w: 123");
    }

    @Test
    public void testExtractSumFromSerializedAverage_preceeding_spaces() {
        double s = AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s:     22.771 w: 123");
        Assert.assertEquals(22.771, s, 0);
    }

    @Test
    public void testExtractSumFromSerializedAverage_trailing_spaces() {
        double s = AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s: 22.772        w: 123");
        Assert.assertEquals(22.772, s, 0);
    }

    @Test
    public void testExtractSumFromSerializedAverage_regular() {
        double s = AggregatedAverageNumberConversionUtil.extractSumFromSerializedAverage("s: 0.123 w: 123");
        Assert.assertEquals(0.123, s, 0);
    }

    //
    // extractWeightsFromSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testExtractWeightsFromSerializedAverage_missing_weights_token() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 22.123 22");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractWeightsFromSerializedAverage_missing_weights_number() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: ");
    }

    @Test(expected = NumberFormatException.class)
    public void testExtractWeightsFromSerializedAverage_weights_not_an_int_scenario1() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: 1.123");
    }

    @Test(expected = NumberFormatException.class)
    public void testExtractWeightsFromSerializedAverage_weights_not_an_int_scenario2() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: blah");
    }

    @Test(expected = NumberFormatException.class)
    public void testExtractWeightsFromSerializedAverage_weights_not_an_int_scenario3() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w:     ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractWeightsFromSerializedAverage_weights_not_a_positive_number() {
        AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: 0");
    }

    @Test
    public void testExtractWeightsFromSerializedAverage_preceding_spaces() {
        int w = AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w:    44");
        Assert.assertEquals(44, w);
    }

    @Test
    public void testExtractWeightsFromSerializedAverage_trailing_spaces() {
        int w = AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: 55    ");
        Assert.assertEquals(55, w);
    }

    @Test
    public void testExtractWeightsFromSerializedAverage_regular() {
        int w = AggregatedAverageNumberConversionUtil.extractWeightsFromSerializedAverage("s: 2342.123 w: 7");
        Assert.assertEquals(7, w);
    }
}
