package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by per on 12/16/14.
 */
public class AggregatedAverageNumberUtilTest {

    // Class under test
    private AggregatedAverageNumberUtil aggregatedAverageNumberUtil;

    @Before
    public void setup() {
        aggregatedAverageNumberUtil = new AggregatedAverageNumberUtil();
    }

    //
    // createNew()
    //

    @Test(expected = IllegalArgumentException.class)
    public void createNew_null_input() {
        aggregatedAverageNumberUtil.createNew(null);
    }

    @Test
    public void createNew_null_truncated() {
        String serialized = aggregatedAverageNumberUtil.createNew(1.3333);
        Assert.assertEquals("{\"s\":\"1.333\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_truncated_and_rounded() {
        String serialized = aggregatedAverageNumberUtil.createNew(1.3337);
        Assert.assertEquals("{\"s\":\"1.334\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_sum_zero_padded() {
        String serialized = aggregatedAverageNumberUtil.createNew(1.1);
        Assert.assertEquals("{\"s\":\"1.100\",\"w\":\"1\"}", serialized);
    }

    @Test
    public void createNew_null_regular() {
        String serialized = aggregatedAverageNumberUtil.createNew(0.111);
        Assert.assertEquals("{\"s\":\"0.111\",\"w\":\"1\"}", serialized);
    }


    //
    // updateSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_average_number() {
        aggregatedAverageNumberUtil.updateSerializedAverage(null, "{\"s\":\"11.11\",\"w\":\"23\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_null_serialized_average() {
        aggregatedAverageNumberUtil.updateSerializedAverage(1.1, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_number_input() {
        aggregatedAverageNumberUtil.updateSerializedAverage(1.1, "{\"s\":\"11.11\",\"w\":\"blah\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateSerializedAverage_weights_not_a_positive_number() {
        aggregatedAverageNumberUtil.updateSerializedAverage(1.1, "{\"s\":\"11.11\",\"w\":\"0\"}");
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated() {
        String serialized = aggregatedAverageNumberUtil.updateSerializedAverage(9.3333, "{\"s\":\"1.000\",\"w\":\"2\"}");
        Assert.assertEquals("{\"s\":\"10.333\",\"w\":\"3\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_truncated_and_rounded() {
        String serialized = aggregatedAverageNumberUtil.updateSerializedAverage(5.6667, "{\"s\":\"1.000\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"6.667\",\"w\":\"2\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_zero_padded() {
        String serialized = aggregatedAverageNumberUtil.updateSerializedAverage(5.44, "{\"s\":\"1.000\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"6.440\",\"w\":\"2\"}", serialized);
    }

    @Test
    public void testUpdateSerializedAverage_sum_regular() {
        String serialized = aggregatedAverageNumberUtil.updateSerializedAverage(0.111, "{\"s\":\"0.111\",\"w\":\"1\"}");
        Assert.assertEquals("{\"s\":\"0.222\",\"w\":\"2\"}", serialized);
    }


    //
    // getAverageFromSerializedAverage()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_null_input() {
        aggregatedAverageNumberUtil.getAverageFromSerializedAverage(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_sum_not_a_number_input() {
        aggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"blah\",\"w\":\"11\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAverageFromSerializedAverage_weights_not_a_positive_number() {
        aggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"11.1234\",\"w\":\"0\"}");
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated() {
        double avg = aggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"22.2222\",\"w\":\"2\"}");
        Assert.assertEquals(11.111, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_average_truncated_and_rounded() {
        double avg = aggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"22.2234\",\"w\":\"2\"}");
        Assert.assertEquals(11.112, avg, 0);
    }

    @Test
    public void testGetAverageFromSerializedAverage_regular() {
        double avg = aggregatedAverageNumberUtil.getAverageFromSerializedAverage("{\"s\":\"0.222\",\"w\":\"2\"}");
        Assert.assertEquals(0.111, avg, 0);
    }
}
