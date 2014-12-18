package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Created by per on 12/17/14.
 */
public class DataPointAggregationUtilTest {

    //
    // updateAggregatedValue()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_null_obj1() {
        DataPointAggregationUtil.updateAggregatedValue(null, new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_null_obj2() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), null, Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_obj1_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue(new HashMap(), new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_obj2_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_unsupported_aggregation() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new Long(2L), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_obj1_long_obj2_double() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new Double(2.2D), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_obj1_double_obj2_long() {
        DataPointAggregationUtil.updateAggregatedValue(new Double(1.0D), new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_obj1_string() {
        DataPointAggregationUtil.updateAggregatedValue("hello", new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_obj2_string() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), "hello", Aggregation.SUM);
    }

    @Test
    public void testUpdateAggregatedValue_long_sum() {
        Object sum = DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new Long(2L), Aggregation.SUM);
        Assert.assertTrue(sum instanceof Long);
        Assert.assertEquals(3L, ((Long)sum).longValue());
    }

    @Test
    public void testUpdateAggregatedValue_double_sum() {
        Object sum = DataPointAggregationUtil.updateAggregatedValue(new Double(1.1D), new Double(2.2D), Aggregation.SUM);
        Assert.assertTrue(sum instanceof Double);
        Assert.assertEquals(3.3D, ((Double)sum).doubleValue(), 0.00001D);
    }

    //
    // sumObjects()
    //

    @Test(expected = IllegalStateException.class)
    public void testSumObjects_obj1_null() {
        DataPointAggregationUtil.sumObjects(null, new Long(2L));
    }

    @Test(expected = IllegalStateException.class)
    public void testSumObjects_obj2_null() {
        DataPointAggregationUtil.sumObjects(new Long(1L), null);
    }

    @Test(expected = IllegalStateException.class)
    public void testSumObjects_obj1_invalid_type() {
        DataPointAggregationUtil.sumObjects("hello", new Long(2L));
    }

    @Test(expected = IllegalStateException.class)
    public void testSumObjects_obj2_invalid_type() {
        DataPointAggregationUtil.sumObjects(new Long(1L), "hello");
    }

    @Test
    public void testSumObjects_long_sum() {
        Object sum = DataPointAggregationUtil.sumObjects(new Long(1L), new Long(2L));
        Assert.assertTrue(sum instanceof Long);
        Assert.assertEquals(3L, ((Long)sum).longValue());
    }

    @Test
    public void testSumObjects_double_sum() {
        Object sum = DataPointAggregationUtil.sumObjects(new Double(1.1D), new Double(2.2D));
        Assert.assertTrue(sum instanceof Double);
        Assert.assertEquals(3.3D, ((Double)sum).doubleValue(), 0.00001D);
    }

    //
    // checkTypeForAggregationUpdate()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_null_obj1() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(null, new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_null_obj2() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), null, Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_obj1_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new HashMap(), new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_obj2_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_unsupported_aggregation() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new Long(2L), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_obj1_long_obj2_double() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new Double(2.2D), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_obj1_double_obj2_long() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Double(1.0D), new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_obj1_string() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate("hello", new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_obj2_string() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), "hello", Aggregation.SUM);
    }
}
