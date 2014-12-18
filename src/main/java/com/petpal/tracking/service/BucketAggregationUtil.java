package com.petpal.tracking.service;

import com.petpal.tracking.service.timeseries.BucketBoundaryUtil;
import com.petpal.tracking.web.controllers.AggregationLevel;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Contains operations to perform calculations on buckets ranges.
 * Created by per on 11/10/14.
 */
@Component
public class BucketAggregationUtil {

    private static final long FORTY_EIGHT_HOURS = 48L*60L*60L*1000L;

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private Logger logger = Logger.getLogger(this.getClass());

    /**
     * If a key present in newDataPoints also exists in existingDataPoints,
     * the two values are added together to create a new value in the returned map.
     * If the key only exists in newDataPoints, the value in the returned map will
     * be equal to the value present for the key in newDataPoints.
     * @param newDataPoints
     * @param existingDataPoints
     * @return a map containing contributions from any existing datapoints incorporated
     * into the new data.
     */
    public TreeMap mergeExistingDataPointsIntoNew(
            TreeMap newDataPoints, TreeMap existingDataPoints, Aggregation aggregation, Type dataType) {

        Assert.isTrue(aggregation == Aggregation.SUM, aggregation + " aggregation not supported");
        Assert.isTrue(dataType == Long.class, dataType + " type not supported");

        TreeMap updatedDataPoints = new TreeMap();

        if(CollectionUtils.isEmpty(existingDataPoints)) {
            logger.info("mergeExistingDataPointsIntoNew(): No existing data points found.");
            if(!CollectionUtils.isEmpty(newDataPoints)) {
                updatedDataPoints.putAll(newDataPoints);
            }
        } else {

            logger.info("mergeExistingDataPointsIntoNew(): Previously aggregated data found => " +
                    "merging in current aggregated data for update using " + aggregation + " aggregation.");

            for(Object newAggregatedDataPointObj : newDataPoints.keySet()) {
                Long newAggregatedDataPoint = (Long) newAggregatedDataPointObj;
                Object newAddedValue = newDataPoints.get(newAggregatedDataPoint);
                Object existingValue = existingDataPoints.get(newAggregatedDataPoint) == null ?
                        new Long(0L) : existingDataPoints.get(newAggregatedDataPoint);
                Object updatedValue = DataPointAggregationUtil.updateAggregatedValue(
                        existingValue, newAddedValue, aggregation);
                updatedDataPoints.put(newAggregatedDataPoint, updatedValue);
            }
        }

        return updatedDataPoints;
    }


    /**
     * Given an input bucket boundary relative to an aggregation timezone, shift the timestamp into
     * UTC and apply the reverse 48 hour shift.
     * @param inputBucketTimeStamp
     * @param timeZone
     * @param aggregationLevel
     * @return a UTC shifted timestamp with a reverse 48 hour shift applied.
     */
    public long getUTCShiftedBucketTimeStamp(long inputBucketTimeStamp, TimeZone timeZone, AggregationLevel aggregationLevel) {
        long utcShiftedBucketTimestamp = getShiftedTimeStamp(inputBucketTimeStamp, timeZone, UTC, aggregationLevel);
        long forthEightHourShiftedUTCBucketTimestamp = shiftTimeStamp(utcShiftedBucketTimestamp, FORTY_EIGHT_HOURS, false);
        return forthEightHourShiftedUTCBucketTimestamp;
    }


    /**
     * Organize tracking data into bucket of the given time unit relative to UTC timezone.
     *
     * Determine the start of the first UTC relative bucket (and thereby the entire series)
     * as follows:
     *
     * - Aggregate the data normally
     * - For each bucket in the aggregated data, shift the bucket start to the UTC relative start of the bucket
     *
     * For example, let's say the aggregation gives the following bucket for aggregation timezone PST
     *
     *   [ts1, 5]
     *
     * A) Find out what year ts1 belongs to in PST, let's say it's 2014.
     * B) Find the UTC timestamp for Jan 1, 2014, 00:00:00 relative to the UTC timezone, call it ts2
     * C) Use ts2 as the bucket into which to add the value 5.
     *
     * In addition to shifting relative to UTC, a forty eight hour reduction is applied to all
     * timestamps in the utc transformed series before returning to avoid the issue of some timestamps
     * potentially being in the future after the shift into UTC.
     *
     * @param trackingMetricConfig information about the metric
     * @param shiftedUnaggregatedData the data to aggregate
     * @param aggregationTimeZone the timezone used to calculate time ranges
     * @param aggregationLevel the bucket size
     *
     * @return data aggregated into the specified bucket size for the given time zone.
     */
    public TreeMap aggregateIntoUTCShiftedBuckets(
            TrackingMetricConfig trackingMetricConfig,
            TreeMap shiftedUnaggregatedData,
            TimeZone aggregationTimeZone,
            AggregationLevel aggregationLevel) {

        TreeMap aggregatedData = convertRawToUTCAggregated(
                trackingMetricConfig, shiftedUnaggregatedData, aggregationTimeZone, aggregationLevel);

        // For each bucket in the aggregated series, shift into a UTC timezone relative buckets
        TreeMap utcShiftedAggregatedData = shiftTimeSeriesToBoundariesForTimeZone(aggregatedData, aggregationTimeZone, UTC, aggregationLevel);

        //
        // Apply a 48hr rewind on all timestamps in the aggregated series to avoid the
        // 'future data' problem after data has been shifted
        //

        TreeMap fortyEightHourShiftedUTCAggregatedData = applyFortyEightHourShift(utcShiftedAggregatedData, false);

        return fortyEightHourShiftedUTCAggregatedData;
    }


    /**
     * Apply the reverse of the UTC shift operation to a query result. This is done before a result for aggregated
     * data can be sent back for representation relative to the aggregation time zone.
     * @param utcRelativeResult
     * @param aggregationTimeZone
     * @param aggregationLevel
     * @return query result presented relative to the aggregation time zone.
     */
    public TreeMap shiftResultToAggregationTimeZone(
            TreeMap utcRelativeResult, TimeZone aggregationTimeZone, AggregationLevel aggregationLevel) {

        // Apply forward 48hr forward shift
        TreeMap fortyEightHourUnshiftedData = applyFortyEightHourShift(utcRelativeResult, true);

        // For each bucket in the aggregated series, shift back into the aggregation timezone
        TreeMap utcShiftedAggregatedData = shiftTimeSeriesToBoundariesForTimeZone(
                fortyEightHourUnshiftedData, UTC, aggregationTimeZone, aggregationLevel);

        return utcShiftedAggregatedData;
    }


    /**
     * Organize the tracking data into buckets of the given time unit.
     *
     * For buckets that are larger than minute-size, the boundaries of the buckets
     * become timezone dependent.
     *
     * For example, if the client has timezone PST, and
     * would like to see buckets grouped into
     *
     *   bucket 1: [00:00:00 may 1, 23:59:59 may 1]
     *   bucket 2: [00:00:00 may 2, 23:59:59 may 2]
     *
     * Our goal is to aggregate the input to this method in such a way that the data
     * can be stored without an aggregation being done at query time. To do that, we
     * have to aggregate the buckets as follows:
     *
     *   bucket 1: UTC timestamp for 00:00:00 may 1 in PST timezone
     *   bucket 2: UTC timestamp for 00:00:00 may 2 in PST timezone
     *
     * @param unaggregatedData the data to aggregate
     * @param timeZone the timezone used to calculate time ranges
     * @param aggregationLevel the bucket size
     *
     * @return data moved into the specified bucket size for the given time zone.
     */
    public TreeMap convertRawToUTCAggregated(
            TrackingMetricConfig trackingMetricConfig, TreeMap unaggregatedData, TimeZone timeZone, AggregationLevel aggregationLevel) {

        if(CollectionUtils.isEmpty(unaggregatedData)) {
            return null;
        }

        Assert.notNull(timeZone, "Timezone not specified for aggregation");
        Assert.notNull(aggregationLevel, "Bucket size aggregation level not specified for aggregation");

        long initialBucketStart = determineInitialBucket((Long) unaggregatedData.keySet().iterator().next(), timeZone, aggregationLevel);

        long currentBucketStart = initialBucketStart;
        long currentBucketEnd = getAggregatedBucketEndTime(currentBucketStart, aggregationLevel, timeZone);
        TreeMap aggregatedData = new TreeMap<Long, Object>();

        for(Object timeStampObj : unaggregatedData.keySet()) {

            Long timeStamp = (Long) timeStampObj;
            Object value = unaggregatedData.get(timeStamp);

            while(!(currentBucketStart <= timeStamp && currentBucketEnd >= timeStamp)) {

                //
                // The value does not belong in the current bucket, make new buckets
                // until we hit the timerange to which this value belongs
                //

                currentBucketStart = currentBucketEnd + 1L;
                currentBucketEnd = getAggregatedBucketEndTime(currentBucketStart, aggregationLevel, timeZone);
            }

            if(aggregatedData.get(currentBucketStart) == null) {
                Object initialValue = DataPointAggregationUtil.initialAggregationValueForBucket(
                        value, trackingMetricConfig.getAggregation());
                aggregatedData.put(currentBucketStart, initialValue);
            } else {
                Object newValue = DataPointAggregationUtil.updateAggregatedValue(
                        aggregatedData.get(currentBucketStart), value, trackingMetricConfig.getAggregation());
                aggregatedData.put(currentBucketStart, newValue);
            }
        }

        return aggregatedData;
    }

    /**
     * Calculate the end time of a bucket given its start time and aggregation level
     * @param bucketStart
     * @param aggregationLevel
     * @param timeZone
     * @return the end time of a bucket
     */
    public static long getAggregatedBucketEndTime(Long bucketStart, AggregationLevel aggregationLevel, TimeZone timeZone) {
        Assert.notNull(aggregationLevel, "Aggregation level not specified");
        TimeUnit timeUnitForAggregationLevel = TimeUnit.valueOf(aggregationLevel.toString().toUpperCase());
        return BucketBoundaryUtil.getBucketEndTime(bucketStart, timeUnitForAggregationLevel, timeZone);
    }

    /**
     * Shifts all the timestamps in a data point map 48 hours forward or backward
     * @param dataPoints
     * @param forward
     * @return a map of datapoints shifted 48 hours in the direction specified
     * by the 'forward' parameter.
     */
    protected TreeMap applyFortyEightHourShift(TreeMap dataPoints, boolean forward) {

        Assert.notEmpty(dataPoints, "Datapoints not specified");

        TreeMap shiftedData = new TreeMap();
        for(Object tsObj : dataPoints.keySet()) {
            Long ts = (Long) tsObj;
            Object newTimeStamp = shiftTimeStamp(ts.longValue(), FORTY_EIGHT_HOURS, forward);
            shiftedData.put(newTimeStamp, dataPoints.get(tsObj));
        }

        return shiftedData;
    }


    /**
     * Shifts the timestamp 48 hours forward or backward
     * @param forward
     * @return a timestamp shifted 48 hours in the direction specified
     * by the 'forward' parameter.
     */
    protected Long shiftTimeStamp(long input, long shiftAmount, boolean forward) {
        long newTimeStamp;
        if(forward) {
            newTimeStamp = input + shiftAmount;
        } else {
            newTimeStamp = input - shiftAmount;
        }
        return newTimeStamp;
    }


    protected TreeMap shiftTimeSeriesToBoundariesForTimeZone(
            TreeMap dataPoints, TimeZone timeZone1, TimeZone timeZone2, AggregationLevel aggregationLevel) {

        TreeMap shiftedData = new TreeMap();
        for(Object timeStampObj : dataPoints.keySet()) {
            Long timeStamp = (Long) timeStampObj;

            Object value = dataPoints.get(timeStampObj);

            long utcShiftedBucketTimestamp = getShiftedTimeStamp(timeStamp, timeZone1, timeZone2, aggregationLevel);

            shiftedData.put(utcShiftedBucketTimestamp, value);
        }

        return shiftedData;
    }

    /**
     * Get a shifted timestamp for a boundary of a given bucket size.
     * @param inputTimeStamp A UTC timestamp
     * @param timeZone1 the timezone to determine the timeunit from.
     * @param timeZone2 the timezone to determine the shifted timestamp relative to.
     * @param aggregationLevel the length of a bucket, e.g. MONTHS.
     * @return a timestamp in UTC identifying the boundary of a bucket in the UTC timezome
     */
    protected long getShiftedTimeStamp(Long inputTimeStamp, TimeZone timeZone1, TimeZone timeZone2, AggregationLevel aggregationLevel) {

        Assert.notNull(inputTimeStamp, "Timestamp not specified");
        Assert.notNull(timeZone1, "Timezone1 not specified");
        Assert.notNull(timeZone2, "Timezone2 not specified");
        Assert.notNull(aggregationLevel, "Aggregation level not specified");

        Calendar inputCal = Calendar.getInstance();
        inputCal.clear();
        inputCal.setTimeZone(timeZone1);
        inputCal.setTimeInMillis(inputTimeStamp);

        //
        // To determine the bucket interval start, reset all the time components that are of
        // less significance than the desired bucket size time unit
        //

        Calendar outputCal = Calendar.getInstance();
        outputCal.clear();
        outputCal.setTimeZone(timeZone2);

        if (aggregationLevel == AggregationLevel.YEARS) {
            outputCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
        } else if (aggregationLevel == AggregationLevel.MONTHS) {
            outputCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            outputCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
        } else if (aggregationLevel == AggregationLevel.WEEKS) {
            outputCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            outputCal.set(Calendar.WEEK_OF_YEAR, inputCal.get(Calendar.WEEK_OF_YEAR));
            outputCal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (aggregationLevel == AggregationLevel.DAYS) {
            outputCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            outputCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
            outputCal.set(Calendar.DAY_OF_MONTH, inputCal.get(Calendar.DAY_OF_MONTH));
        } else if (aggregationLevel == AggregationLevel.HOURS) {
            outputCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            outputCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
            outputCal.set(Calendar.DAY_OF_MONTH, inputCal.get(Calendar.DAY_OF_MONTH));
            outputCal.set(Calendar.HOUR_OF_DAY, inputCal.get(Calendar.HOUR_OF_DAY));
        } else {
            throw new IllegalArgumentException("Unexpected aggregation level " + aggregationLevel);
        }

        return outputCal.getTimeInMillis();
    }


    /**
     * Determine the timestamp for the beginning of the bucket into which the data for
     * a given timestamp would be aggregated.
     * @param initialTimeStamp A UTC timestamp for which some data is to be aggregated into a bucket
     * @param timeZone the boundary point for a bucket in UTC is dependent on what timezones the client
     *                 wants to view the aggregated data relative to.
     * @param aggregationLevel the length of a bucket, e.g. MONTHS.
     * @return a timestamp in UTC identifying the start of the bucket into which the data for the
     * given timestamp will be aggregated.
     */
    protected long determineInitialBucket(Long initialTimeStamp, TimeZone timeZone, AggregationLevel aggregationLevel) {

        Assert.notNull(initialTimeStamp, "Initial timestamp not specified");
        Assert.notNull(timeZone, "Timezone not specified");
        Assert.notNull(aggregationLevel, "Aggregation level not specified");

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);
        cal.setTimeInMillis(initialTimeStamp);

        //
        // To determine the bucket interval start, reset all the time components that are of
        // less significance than the desired bucket size time unit
        //

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZone);

        if (aggregationLevel == AggregationLevel.YEARS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
        } else if (aggregationLevel == AggregationLevel.MONTHS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
        } else if (aggregationLevel == AggregationLevel.WEEKS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR));
            bucketStartCal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (aggregationLevel == AggregationLevel.DAYS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
        } else if (aggregationLevel == AggregationLevel.HOURS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
            bucketStartCal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY));
        } else {
            throw new IllegalArgumentException("Unexpected aggregation level " + aggregationLevel);
        }

        return bucketStartCal.getTimeInMillis();
    }
}
