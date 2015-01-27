package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Created by per on 12/17/14.
 */
public class DataPointAggregationUtilTest {

    //
    // initialAggregationValueForBucket
    //

    @Test(expected = IllegalArgumentException.class)
    public void testInitialAggregationValueForBucket_value_null() {
        DataPointAggregationUtil.initialAggregationValueForBucket(null, Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialAggregationValueForBucket_illegal_type_for_value() {
        DataPointAggregationUtil.initialAggregationValueForBucket(new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialAggregationValueForBucket_sum_string_type_not_allowed() {
        DataPointAggregationUtil.initialAggregationValueForBucket("hello", Aggregation.SUM);
    }

    @Test
    public void testInitialAggregationValueForBucket_sum_long_echo() {
        Object initialValue = DataPointAggregationUtil.initialAggregationValueForBucket(new Long(744L), Aggregation.SUM);
        Assert.assertTrue(initialValue instanceof Long);
        Assert.assertEquals(744L, ((Long) initialValue).longValue());
    }

    @Test
    public void testInitialAggregationValueForBucket_sum_double_echo() {
        Object initialValue = DataPointAggregationUtil.initialAggregationValueForBucket(new Double(744.1D), Aggregation.SUM);
        Assert.assertTrue(initialValue instanceof Double);
        Assert.assertEquals(744.1D, ((Double) initialValue).doubleValue(), 0.0D);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialAggregationValueForBucket_average_string_not_supported() {
        DataPointAggregationUtil.initialAggregationValueForBucket("string", Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitialAggregationValueForBucket_average_long_not_supported() {
        DataPointAggregationUtil.initialAggregationValueForBucket(new Long(5L), Aggregation.AVERAGE);
    }

    @Test
    public void testInitialAggregationValueForBucket_average_serialized() {
        Object initialValue = DataPointAggregationUtil.initialAggregationValueForBucket(new Double(5.555D), Aggregation.AVERAGE);
        Assert.assertTrue(initialValue instanceof String);
        Assert.assertEquals("{\"s\":\"5.555\",\"w\":\"1\"}", initialValue.toString());
    }


    //
    // initializeAggregatedValue()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeAggregatedValue_sum_illegal_type() {
        DataPointAggregationUtil.initializeAggregatedValue(new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeAggregatedValue_sum_invalid_type_for_sum() {
        DataPointAggregationUtil.initializeAggregatedValue("hello", Aggregation.SUM);
    }

    @Test
    public void testInitializeAggregatedValue_sum_expected_value() {
        Object initValue = DataPointAggregationUtil.initializeAggregatedValue(new Long(777L), Aggregation.SUM);
        Assert.assertEquals(new Long(777), initValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeAggregatedValue_avg_illegal_type() {
        DataPointAggregationUtil.initializeAggregatedValue(new HashMap(), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeAggregatedValue_avg_invalid_type_for_sum() {
        DataPointAggregationUtil.initializeAggregatedValue("hello", Aggregation.AVERAGE);
    }

    @Test
    public void testInitializeAggregatedValue_avg_type_ok() {
        Object initValue = DataPointAggregationUtil.initializeAggregatedValue(new Double(1.1D), Aggregation.AVERAGE);
        Assert.assertEquals("{\"s\":\"1.100\",\"w\":\"1\"}", initValue);
    }

    //
    // updateAggregatedValue()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_null_new_value() {
        DataPointAggregationUtil.updateAggregatedValue(null, new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_null_existing_value() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), null, Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_new_value_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue(new HashMap(), new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_existing_value_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_sum_new_value_long_existing_value_double() {
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
    public void testUpdateAggregatedValue_avg_long_sum() {
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

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_avg_existing_value_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue(new Long(1L), new Double(2.2D), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateAggregatedValue_avg_new_value_illegal_type() {
        DataPointAggregationUtil.updateAggregatedValue("hello", new Long(2L), Aggregation.AVERAGE);
    }

    @Test
    public void testUpdateAggregatedValue_avg_updated_average() {
        Object updated = DataPointAggregationUtil.updateAggregatedValue(
                "{\"s\":\"8.0\",\"w\":\"1\"}", new Double(3.0D), Aggregation.AVERAGE);
        Assert.assertEquals("{\"s\":\"11.000\",\"w\":\"2\"}", updated.toString());
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
    // checkTypeAggregationInitialization()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeAggregationInitialization_sum_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization(new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeAggregationInitialization_sum_invalid_type_for_sum() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization("hello", Aggregation.SUM);
    }

    @Test
    public void testCheckTypeAggregationInitialization_sum_type_ok() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization(new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeAggregationInitialization_avg_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization(new HashMap(), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeAggregationInitialization_avg_invalid_type_for_sum() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization("hello", Aggregation.AVERAGE);
    }

    @Test
    public void testCheckTypeAggregationInitialization_avg_type_ok() {
        DataPointAggregationUtil.checkTypeForAggregationInitialization(new Double(1.1D), Aggregation.AVERAGE);
    }


    //
    // checkTypeForAggregationUpdate()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_new_value_is_null() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(null, new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_null_existing_value() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), null, Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_new_value_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new HashMap(), new Long(1L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_existing_value_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new HashMap(), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_new_value_long_existingValue_double() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new Double(2.2D), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_new_value_double_existing_value_long() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Double(1.0D), new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_new_value_string() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate("hello", new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_sum_existing_value_string() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), "hello", Aggregation.SUM);
    }

    @Test
    public void testCheckTypeForAggregationUpdate_sum_pass() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new Long(2L), Aggregation.SUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_avg_existing_value_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate(new Long(1L), new Double(2.2D), Aggregation.AVERAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckTypeForAggregationUpdate_avg_new_value_illegal_type() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate("hello", new Long(1L), Aggregation.AVERAGE);
    }

    @Test
    public void testCheckTypeForAggregationUpdate_avg_pass() {
        DataPointAggregationUtil.checkTypeForAggregationUpdate("hello", new Double(2.2D), Aggregation.AVERAGE);
    }
}
