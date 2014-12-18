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
            // TODO: Support roll-up of average into string
            throw new IllegalArgumentException("Aggregation " + Aggregation.AVERAGE + " not supported");
        } else {
            throw new IllegalStateException("Unexpected aggregation " + aggregation);
        }
    }


    protected static Object sumObjects(Object o1, Object o2) {
        if((o1 instanceof Long) && (o2 instanceof Long)) {
            return new Long(((Long) o1).longValue() + ((Long) o2).longValue());
        } else if((o1 instanceof Double) && (o2 instanceof Double)) {
            return new Double(((Double) o1).doubleValue() + ((Double) o2).doubleValue());
        } else {
            String o1Type = (o1 == null) ? null : o1.getClass().toString();
            String o2Type = (o2 == null) ? null : o2.getClass().toString();
            throw new IllegalStateException("Unexpected types. o1 = " + o1 + " (" + o1Type +
                    "), o2 = " + o2 + " (" + o2Type + ")");
        }
    }


    protected static void checkTypeForAggregationUpdate(Object obj1, Object obj2, Aggregation aggregation) {

        checkNotIllegalType(obj1);
        checkNotIllegalType(obj2);

        if(aggregation == Aggregation.SUM) {

            if((obj1 instanceof Long) && !(obj2 instanceof Long)) {
                throw new IllegalArgumentException(obj1 + " is a Long, but " + obj2 + " is not a long");
            }

            if((obj1 instanceof Double) && !(obj2 instanceof Double)) {
                throw new IllegalArgumentException(obj1 + " is a Double, but " + obj2 + " is not a Double");
            }

            if((obj1 instanceof String) || (obj2 instanceof String)) {
                throw new IllegalArgumentException("Sum aggregation update of data points not allowed " +
                        "datapoints of type String, obj1 = " + obj1 + ", obj2 = " + obj2);
            }

        } else if(aggregation == Aggregation.AVERAGE) {
            // TODO: Support roll-up of average into string
            throw new IllegalArgumentException("Aggregation " + Aggregation.AVERAGE + " not supported");
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
