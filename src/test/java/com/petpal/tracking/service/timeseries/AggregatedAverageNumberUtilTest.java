package com.petpal.tracking.service.timeseries;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by per on 12/16/14.
 */
public class AggregatedAverageNumberUtilTest {

    //
    // createNew()
    //

    @Test(expected = IllegalArgumentException.class)
    public void createNew_null_input() {
        AggregatedAverageNumberUtil.createNew(null);
    }

    @Test
    public void createNew_null_truncated() {
        String serialized = AggregatedAverageNumberUtil.createNew(1.3333);
        Assert.assertEquals("{\"s\":\"1.333\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_truncated_and_rounded() {
        String serialized = AggregatedAverageNumberUtil.createNew(1.3337);
        Assert.assertEquals("{\"s\":\"1.334\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_sum_zero_padded() {
        String serialized = AggregatedAverageNumberUtil.createNew(1.1);
        Assert.assertEquals("{\"s\":\"1.100\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_regular() {
        String serialized = AggregatedAverageNumberUtil.createNew(0.111);
        Assert.assertEquals("{\"s\":\"0.111\",\"w\":\"1\"}", serialized);
    }


    //
    // updateSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_average_number() {
        AggregatedAverageNumberUtil.updateSerializedAverage(null, "{\"s\":\"11.11\",\"w\":\"23\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_serialized_average() {
        AggregatedAverageNumberUtil.updateSerializedAverage(1.1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_number_input() {
        AggregatedAverageNumberUtil.updateSerializedAverage(1.1, "{\"s\":\"11.11\",\"w\":\"blah\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_positive_number() {
        AggregatedAverageNumberUtil.updateSerializedAverage(1.1, "{\"s\":\"11.11\",\"w\":\"0\"}");
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated() {
        String serialized = AggregatedAverageNumberUtil.updateSerializedAverage(9.3333, "{\"s\":\"1.000\",\"w\":\"2\"}");
        Assert.assertEquals("{\"s\":\"10.333\",\"w\":\"3\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated_and_rounded() {
        String serialized = AggregatedAverageNumberUtil.updateSerializedAverage(5.6667, "{\"s\":\"1.000\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"6.667\",\"w\":\"2\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_zero_padded() {
        String serialized = AggregatedAverageNumberUtil.updateSerializedAverage(5.44, "{\"s\":\"1.000\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"6.440\",\"w\":\"2\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_regular() {
        String serialized = AggregatedAverageNumberUtil.updateSerializedAverage(0.111, "{\"s\":\"0.111\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"0.222\",\"w\":\"2\"}", serialized);
    }


    //
    // getAverageFromSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_null_input() {
        AggregatedAverageNumberUtil.getAverageFromSerializedAverage(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_sum_not_a_number_input() {
        AggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"blah\",\"w\":\"11\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_weights_not_a_positive_number() {
        AggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"11.1234\",\"w\":\"0\"}");
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated() {
        double avg = AggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"22.2222\",\"w\":\"2\"}");
        Assert.assertEquals(11.111, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated_and_rounded() {
        double avg = AggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"22.2234\",\"w\":\"2\"}");
        Assert.assertEquals(11.112, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_regular() {
        double avg = AggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"0.222\",\"w\":\"2\"}");
        Assert.assertEquals(0.111, avg, 0);
    }

}
