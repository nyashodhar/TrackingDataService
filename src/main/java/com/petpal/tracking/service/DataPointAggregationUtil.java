package com.petpal.tracking.service;

import org.springframework.util.Assert;

/**
 * Created by per on 12/17/14.
 */
public class DataPointAggregationUtil {

    private static AggregatedAverageNumberUtil aggregatedAverageNumberUtil;

    static {
        aggregatedAverageNumberUtil = new AggregatedAverageNumberUtil();
    }

    public static Object initialAggregationValueForBucket(Object value, Aggregation aggregation) {

        checkNotIllegalType(value);

        if (aggregation == Aggregation.SUM) {

            if(value instanceof String) {
                throw new IllegalArgumentException("String not allowed as initial value for aggregation bucket for " + aggregation);
            }

            if(value instanceof Long) {
                return value;
            } else if(value instanceof Double) {
                return value;
            } else {
                throw new IllegalStateException("Unable to determine initial " +
                        aggregation + " aggregation bucket value for value " + value);
            }
        } else if (aggregation == Aggregation.AVERAGE) {
            Assert.isTrue(!(value instanceof Long),
                    "Long value " + value + " not allowed as initial input value for " + aggregation + " aggregation bucket");
            Assert.isTrue(!(value instanceof String),
                    "String value " + value + " not allowed as initial input value for " + aggregation + " aggregation bucket");
            return aggregatedAverageNumberUtil.createNew((Double) value);
        } else {
            throw new IllegalStateException("Unexpected aggregation " + aggregation);
        }
    }

    public static Object updateAggregatedValue(Object existingValue, Object objectToAdd, Aggregation aggregation) {

        Assert.notNull(existingValue, "Existing value can't be null");
        Assert.notNull(objectToAdd, "Object to add can't be null");
        checkTypeForAggregationUpdate(existingValue, objectToAdd, aggregation);

        if (aggregation == Aggregation.SUM) {
            return sumObjects(existingValue, objectToAdd);
        } else if (aggregation == Aggregation.AVERAGE) {
            Double addedValue = (Double) objectToAdd;
            return aggregatedAverageNumberUtil.updateSerializedAverage(existingValue.toString(), addedValue);
        } else {
            throw new IllegalStateException("Unexpected aggregation " + aggregation);
        }
    }


    protected static Object sumObjects(Object existingValue, Object addedValue) {
        if((existingValue instanceof Long) && (addedValue instanceof Long)) {
            return new Long(((Long) existingValue).longValue() + ((Long) addedValue).longValue());
        } else if((existingValue instanceof Double) && (addedValue instanceof Double)) {
            return new Double(((Double) existingValue).doubleValue() + ((Double) addedValue).doubleValue());
        } else {
            String existingValueType = (existingValue == null) ? null : existingValue.getClass().toString();
            String addedValueType = (addedValue == null) ? null : addedValue.getClass().toString();
            throw new IllegalStateException("Unexpected types. existingValue = " + existingValue + " (" + existingValueType +
                    "), o2 = " + addedValue + " (" + addedValueType + ")");
        }
    }

    protected static void checkTypeForAggregationUpdate(Object existingVal, Object newVal, Aggregation aggregation) {

        checkNotIllegalType(newVal);
        checkNotIllegalType(existingVal);

        if(aggregation == Aggregation.SUM) {

            if((newVal instanceof Long) && !(existingVal instanceof Long)) {
                throw new IllegalArgumentException(newVal + " is a Long, but " + existingVal + " is not a long");
            }

            if((newVal instanceof Double) && !(existingVal instanceof Double)) {
                throw new IllegalArgumentException(newVal + " is a Double, but " + existingVal + " is not a Double");
            }

            if((newVal instanceof String) || (existingVal instanceof String)) {
                throw new IllegalArgumentException("Sum aggregation update of data points not allowed " +
                        "datapoints of type String, newVal = " + newVal + ", existingVal = " + existingVal);
            }

        } else if(aggregation == Aggregation.AVERAGE) {

            if(!(newVal instanceof Double)) {
                throw new IllegalArgumentException("newVal (" + newVal + " is not a Double");
            }

            if(!(existingVal instanceof String)) {
                throw new IllegalArgumentException("existingVal (" + existingVal + " is not a String");
            }

        } else {
            throw new IllegalStateException("Unexpected aggregation " + aggregation);
        }
    }

    private static void checkNotIllegalType(Object obj) {
        if(!(obj instanceof Long) && !(obj instanceof Double) && !(obj instanceof String)) {
            throw new IllegalArgumentException("Object " + obj + " has illegal type");
        }
    }

}
