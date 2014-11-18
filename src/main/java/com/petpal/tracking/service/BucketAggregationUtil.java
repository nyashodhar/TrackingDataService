package com.petpal.tracking.service;

import com.petpal.tracking.service.metrics.TimeSeriesMetric;
import com.petpal.tracking.service.util.QueryLoggingUtil;
import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public Map<Long, Long> mergeExistingDataPointsIntoNew(
            Map<Long, Long> newDataPoints, Map<Long, Long> existingDataPoints) {

        Map<Long, Long> updatedDataPoints = new TreeMap<Long, Long>();

        if(CollectionUtils.isEmpty(existingDataPoints)) {
            logger.info("mergeExistingDataPointsIntoNew(): No existing data points found.");
            if(!CollectionUtils.isEmpty(newDataPoints)) {
                updatedDataPoints.putAll(newDataPoints);
            }
        } else {

            logger.info("mergeExistingDataPointsIntoNew(): Previously aggregated data found => " +
                    "merging in current aggregated data for update.");

            for(long newAggregatedDataPoint : newDataPoints.keySet()) {
                long newAddedValue = newDataPoints.get(newAggregatedDataPoint);
                long existingValue = existingDataPoints.get(newAggregatedDataPoint) == null ?
                        0L : existingDataPoints.get(newAggregatedDataPoint);
                long updatedValue = newAddedValue + existingValue;
                updatedDataPoints.put(newAggregatedDataPoint, updatedValue);
            }
        }

        return updatedDataPoints;
    }


    /**
     * Shifts all the timestamps in a data point map 48 hours forward or backward
     * @param dataPoints
     * @param forward
     * @return a map of datapoints shifted 48 hours in the direction specified
     * by the 'forward' parameter.
     */
    protected Map<Long, Long> applyFortyEightHourShift(Map<Long, Long> dataPoints, boolean forward) {

        if(CollectionUtils.isEmpty(dataPoints)) {
            throw new IllegalArgumentException("Datapoints not specified");
        }

        Map<Long, Long> shiftedData = new TreeMap();
        for(long ts : dataPoints.keySet()) {
            long newTimeStamp = shiftTimeStamp(ts, FORTY_EIGHT_HOURS, forward);
            shiftedData.put(newTimeStamp, dataPoints.get(ts));
        }

        return shiftedData;
    }


    /**
     * Shifts the timestamp 48 hours forward or backward
     * @param forward
     * @return a timestamp shifted 48 hours in the direction specified
     * by the 'forward' parameter.
     */
    protected long shiftTimeStamp(long input, long shiftAmount, boolean forward) {
        long newTimeStamp;
        if(forward) {
            newTimeStamp = input + shiftAmount;
        } else {
            newTimeStamp = input - shiftAmount;
        }
        return newTimeStamp;
    }


    protected Map<Long, Long> shiftTimeSeriesToBoundariesForTimeZone(Map<Long, Long> dataPoints, TimeZone timeZone1, TimeZone timeZone2, TimeUnit bucketSize) {
        // For each bucket in the aggregated series, into a UTC timezone relative buckets

        TreeMap<Long, Long> shiftedData = new TreeMap<Long, Long>();
        for(long timeStamp : dataPoints.keySet()) {
            long utcShiftedBucketTimestamp = getShiftedTimeStamp(timeStamp, timeZone1, timeZone2, bucketSize);
            shiftedData.put(utcShiftedBucketTimestamp, dataPoints.get(timeStamp));
        }

        return shiftedData;
    }

    /**
     * Given an input bucket boundary relative to an aggregation timezone, shift the timestamp into
     * UTC and apply the reverse 48 hour shift.
     * @param inputBucketTimeStamp
     * @param timeZone
     * @param bucketSize
     * @return a UTC shifted timestamp with a reverse 48 hour shift applied.
     */
    public long getUTCShiftedBucketTimeStamp(long inputBucketTimeStamp, TimeZone timeZone, TimeUnit bucketSize) {
        long utcShiftedBucketTimestamp = getShiftedTimeStamp(inputBucketTimeStamp, timeZone, UTC, bucketSize);
        long forthEightHourShiftedUTCBucketTimestamp = shiftTimeStamp(utcShiftedBucketTimestamp, FORTY_EIGHT_HOURS, false);
        return forthEightHourShiftedUTCBucketTimestamp;
    }


    /**
     * Get a shifted timestamp for a boundary of a given bucket size.
     * @param inputTimeStamp A UTC timestamp
     * @param timeZone1 the timezone to determine the timeunit from.
     * @param timeZone2 the timezone to determine the shifted timestamp relative to.
     * @param bucketSize the length of a bucket, e.g. MONTHS.
     * @return a timestamp in UTC identifying the boundary of a bucket in the UTC timezome
     */
    protected long getShiftedTimeStamp(Long inputTimeStamp, TimeZone timeZone1, TimeZone timeZone2, TimeUnit bucketSize) {

        if(inputTimeStamp == null) {
            throw new IllegalArgumentException("Timestamp not specified");
        }

        if(timeZone1 == null) {
            throw new IllegalArgumentException("Timezone1 not specified");
        }

        if(timeZone2 == null) {
            throw new IllegalArgumentException("Timezone2 not specified");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucketsize not specified");
        }

        Calendar inputCal = Calendar.getInstance();
        inputCal.clear();
        inputCal.setTimeZone(timeZone1);
        inputCal.setTimeInMillis(inputTimeStamp);

        //
        // To determine the bucket interval start, reset all the time components that are of
        // less significance than the desired bucket size time unit
        //

        Calendar utcShiftedBucketStartCal = Calendar.getInstance();
        utcShiftedBucketStartCal.clear();
        utcShiftedBucketStartCal.setTimeZone(timeZone2);

        if (bucketSize == TimeUnit.YEARS) {
            utcShiftedBucketStartCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
        } else if (bucketSize == TimeUnit.MONTHS) {
            utcShiftedBucketStartCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            utcShiftedBucketStartCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
        } else if (bucketSize == TimeUnit.WEEKS) {
            utcShiftedBucketStartCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            utcShiftedBucketStartCal.set(Calendar.WEEK_OF_YEAR, inputCal.get(Calendar.WEEK_OF_YEAR));
            utcShiftedBucketStartCal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (bucketSize == TimeUnit.DAYS) {
            utcShiftedBucketStartCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            utcShiftedBucketStartCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
            utcShiftedBucketStartCal.set(Calendar.DAY_OF_MONTH, inputCal.get(Calendar.DAY_OF_MONTH));
        } else if (bucketSize == TimeUnit.HOURS) {
            utcShiftedBucketStartCal.set(Calendar.YEAR, inputCal.get(Calendar.YEAR));
            utcShiftedBucketStartCal.set(Calendar.MONTH, inputCal.get(Calendar.MONTH));
            utcShiftedBucketStartCal.set(Calendar.DAY_OF_MONTH, inputCal.get(Calendar.DAY_OF_MONTH));
            utcShiftedBucketStartCal.set(Calendar.HOUR_OF_DAY, inputCal.get(Calendar.HOUR_OF_DAY));
        } else if (bucketSize == TimeUnit.MINUTES) {
            //
            // Note: TimeUnit MINUTES should not be used here. Pre-aggregation is only needed
            // applicable for timeunits larger than MINUTES.
            //
            throw new IllegalArgumentException("TimeUnit MINUTES not supported for pre-aggregration.");
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit " + bucketSize);
        }

        return utcShiftedBucketStartCal.getTimeInMillis();
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
     * For example, let's say the aggregation gives the following bucket
     *
     *   [ts1, 5]
     *
     * A) Find out what year ts1 belongs to in PDT, let's say it's 2014.
     * B) Find the UTC timestamp for Jan 1, 2014, 00:00:00 relative to the UTC timezone, call it ts2
     * C) Use ts2 as the bucket into which to add the value 5.
     *
     * In addition to shifting relative to UTC, a forty eight hour reduction is applied to all
     * timestamps in the utc transformed series before returning to avoid the issue of some timestamps
     * potentially being in the future after the shift into UTC.
     *
     * @param shiftedUnaggregatedData the data to aggregate
     * @param aggregationTimeZone the timezone used to calculate time ranges
     * @param bucketSize the bucket size
     *
     * @return data aggregated into the specified bucket size for the given time zone.
     */
    public Map<Long, Long> aggregateIntoUTCShiftedBuckets(Map<Long, Long> shiftedUnaggregatedData, TimeZone aggregationTimeZone, TimeUnit bucketSize) {

        TreeMap<Long, Long> aggregatedData =
                aggregateIntoBucketsForTimeZone(shiftedUnaggregatedData, aggregationTimeZone, bucketSize);

        // For each bucket in the aggregated series, into a UTC timezone relative buckets

        Map<Long, Long> utcShiftedAggregatedData = shiftTimeSeriesToBoundariesForTimeZone(aggregatedData, aggregationTimeZone, UTC, bucketSize);

        //
        // Apply a 48hr rewind on all timestamps in the aggregated series to avoid the
        // 'future data' problem after data has been shifted
        //

        Map<Long, Long> fortyEightHourShiftedUTCAggregatedData = applyFortyEightHourShift(utcShiftedAggregatedData, false);

        return fortyEightHourShiftedUTCAggregatedData;
    }


    /**
     * Apply the reverse of the UTC shift operation to a query result. This is done before a result for aggregated
     * data can be sent back for representation relative to the aggregation time zone.
     * @param utcRelativeResult
     * @param aggregationTimeZone
     * @param bucketSize
     * @return query result presented relative to the aggregation time zone.
     */
    public Map<Long, Long> shiftResultToAggregationTimeZone(Map<Long, Long> utcRelativeResult, TimeZone aggregationTimeZone, TimeUnit bucketSize) {

        // Apply forward 48hr forward shift
        Map<Long, Long> fortyEightHourUnshiftedData = applyFortyEightHourShift(utcRelativeResult, true);

        // For each bucket in the aggregated series, shift back into the aggregation timezone
        Map<Long, Long> utcShiftedAggregatedData = shiftTimeSeriesToBoundariesForTimeZone(fortyEightHourUnshiftedData, UTC, aggregationTimeZone, bucketSize);

        return utcShiftedAggregatedData;
    }



    /**
     * Organize tracking data into bucket of the given time unit.
     *
     * For buckets that are larger than minute-size, the boundaries of the buckets
     * become timezone dependent.
     *
     * For example, if the client has timezone PDT, and
     * would like to see buckets grouped into
     *
     *   bucket 1: [00:00:00 may 1, 23:59:59 may 1]
     *   bucket 2: [00:00:00 may 2, 23:59:59 may 2]
     *
     * Our goal is to aggregate the input to this method in such a way that the data
     * can be stored without an aggregation being done at query time. To do that, we
     * have to aggregate the buckets as follows:
     *
     *   bucket 1: UTC timestamp for 00:00:00 may 1 in PDT timezone
     *   bucket 2: UTC timestamp for 00:00:00 may 2 in PDT timezone
     *
     * @param unaggregatedData the data to aggregate
     * @param timeZone the timezone used to calculate time ranges
     * @param bucketSize the bucket size
     *
     * @return data aggregated into the specified bucket size for the given time zone.
     */
    public TreeMap<Long, Long> aggregateIntoBucketsForTimeZone(Map<Long, Long> unaggregatedData, TimeZone timeZone, TimeUnit bucketSize) {

        if (CollectionUtils.isEmpty(unaggregatedData)) {
            return null;
        }

        if(timeZone == null) {
            throw new IllegalArgumentException("Timezone not specified for aggregation");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucket size time unit not specified for aggregation");
        }

        // Ensure the unaggregated data is sorted by timestamp

        TreeMap<Long, Long> sortedUnaggregatedData = new TreeMap<Long, Long>();
        sortedUnaggregatedData.putAll(unaggregatedData);

        long initialBucketStart = determineInitialBucket(unaggregatedData.keySet().iterator().next(), timeZone, bucketSize);

        long currentBucketStart = initialBucketStart;
        long currentBucketEnd = getBucketEndTime(currentBucketStart, bucketSize, timeZone);
        TreeMap<Long, Long> aggregatedData = new TreeMap<Long, Long>();
        aggregatedData.put(currentBucketStart, 0L);

        for(Long timeStamp : sortedUnaggregatedData.keySet()) {

            long value = sortedUnaggregatedData.get(timeStamp);

            while(!(currentBucketStart <= timeStamp && currentBucketEnd >= timeStamp)) {
                //
                // The value does not belong in the current bucket, make new buckets
                // until we hit the timerange to which this value belongs
                //

                currentBucketStart = currentBucketEnd + 1L;
                currentBucketEnd = getBucketEndTime(currentBucketStart, bucketSize, timeZone);
                //aggregatedData.put(currentBucketStart, 0L);
            }

            //
            // At this point at least one value will be injected into the current bucket.
            // Make sure it is created.
            //

            if(aggregatedData.get(currentBucketStart) == null) {
                aggregatedData.put(currentBucketStart, 0L);
            }

            // Add the value to the bucket
            long newValue = aggregatedData.get(currentBucketStart) + value;
            aggregatedData.put(currentBucketStart, newValue);
        }

        return aggregatedData;
    }


    public void adjustBucketBoundaries(Map<TimeSeriesMetric, Map<Long, Long>> metricResults,
                                        Long utcBegin, Long utcEnd, TimeUnit resultBucketSize, boolean verboseResponse) {

        for (TimeSeriesMetric timeSeriesMetric : metricResults.keySet()) {

            //
            // Adjust the bucket boundaries and inject empty buckets.
            //
            Map<Long, Long> adjustedMetricResult = adjustBoundariesForMetricResult(
                    metricResults.get(timeSeriesMetric), utcBegin, utcEnd, resultBucketSize);
            metricResults.put(timeSeriesMetric, adjustedMetricResult);

            logger.debug("Bucket adjust for time series metric " + timeSeriesMetric + ": old bucket count = " +
                    metricResults.get(timeSeriesMetric).size() + ", new bucket count = " + adjustedMetricResult.size());

            // If the response is not to be verbose, filter out the empty buckets.
            if(!verboseResponse) {
                Set<Long> bucketTimeStamps = new HashSet<Long>(adjustedMetricResult.keySet());
                for(Long timestamp : bucketTimeStamps) {
                    if(adjustedMetricResult.get(timestamp) == 0L) {
                        adjustedMetricResult.remove(timestamp);
                    }
                }
            }
        }
    }


    /**
     * When aggregating the output of a query into buckets the timestamp of the bucket is
     * unfortunately NOT equal to the start of the bucket interval. Rather the timestamp
     * of the sum in a bucket aggregated from a query equal to the first timestamp for
     * which a datapoint found in the bucket interval.
     *
     * This method adjusts the bucket starts of each bucket to align with boundaries of the
     * size indicated by the result bucket size.
     *
     * For bucket intervals for which no data was found, the result does not include a bucket.
     * This method will insert an empty bucket tied to the start of the interval for which
     * no data was found.
     *
     * Example:
     *    Database has datapoints May 29 - 3 steps, July 2nd 2 steps
     * Query:
     *    Get data starting May 1st - Aug 1
     * Result:
     *    Bucket 1 - timestamp May 29 - value/sum = 3
     *    Bucket 2 - timestamp July 2nd - value/sum = 3
     * After adjustment in this method:
     *    Bucket 1 - timestamp May 1 - value/sum = 3
     *    Bucket 2 - timestamp June 1 - value/sum = 0
     *    Bucket 2 - timestamp July 1 - value/sum = 3
     *
     * @param metricResult the result that is to have its bucket boundaries adjusted
     * @param utcBegin the start of the query interval, this also implicitly defines the
     *                 start interval for the first bucket in the result and by extension
     *                 the interval for every bucket to follow. utcBegin must be in the past.
     * @param utcEnd the end of the query interval. Can be null, in which case 'now' will be the
     *               implicit end of the query interval. If specificed, utcEnd must
     *               be in the past and AFTER utcBegin.
     * @param resultBucketSize the length of the time interval covered by each bucket.
     * @return buckets with the same values, but adjusted bucket intervals to align with
     * utcBegin and the specific result bucket size.
     */
    public Map<Long, Long> adjustBoundariesForMetricResult(
            Map<Long, Long> metricResult, Long utcBegin, Long utcEnd, TimeUnit resultBucketSize) {

        if(utcBegin == null) {
            throw new IllegalArgumentException("utcBegin not specified");
        }

        if(utcEnd != null && (utcEnd.longValue() <= utcBegin.longValue())) {
            throw new IllegalArgumentException("The utcEnd value ("+utcEnd+") must be greater than utcBegin value (" + utcBegin + ")");
        }

        Calendar cursorCalendar = new GregorianCalendar();
        cursorCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        cursorCalendar.setTimeInMillis(utcBegin);

        long endOfInterval = (utcEnd == null) ? System.currentTimeMillis() : utcEnd;

        //
        // Generate all the correct bucket boundaries
        // This will also ensure that any missing empty buckets are created.
        //

        Map<Long, Long> newMetricResult = new TreeMap<Long, Long>();

        while(true) {
            newMetricResult.put(cursorCalendar.getTimeInMillis(), 0L);

            //
            // Step one bucket size unit forward. If we're beyond
            // the end of the interval don't add any more buckets.
            //
            //int calenderUnitToStepForward = getCalendarUnitForTimeUnit(resultBucketSize);
            //cursorCalendar.add(calenderUnitToStepForward, 1);

            long bucketEnd = getBucketEndTime(cursorCalendar.getTimeInMillis(), resultBucketSize, cursorCalendar.getTimeZone());
            cursorCalendar.setTimeInMillis(bucketEnd+1);

            if(cursorCalendar.getTimeInMillis() >= endOfInterval) {
                break;
            }
        }

        logTimeStampAdjustments(metricResult, newMetricResult);

        //
        // Walk through all the previous results, and drop them into the correct corresponding bucket
        // TODO: Since it's a treemap, we can do this in linear time but for now just brute attack...
        //

        List<Long> newBucketTimeStamps = new ArrayList<Long>(newMetricResult.size());
        newBucketTimeStamps.addAll(newMetricResult.keySet());

        List<LongRange> newBucketRanges = new ArrayList<LongRange>(newMetricResult.size());

        for (int i = 0; i < newBucketTimeStamps.size(); i++) {
            long timeStampAtIndex = newBucketTimeStamps.get(i);
            if(i == (newBucketTimeStamps.size()-1)) {
                newBucketRanges.add(new LongRange(timeStampAtIndex, endOfInterval));
            } else {
                newBucketRanges.add(new LongRange(timeStampAtIndex, newBucketTimeStamps.get(i+1)-1));
            }
        }

        for(long oldTimeStamp : metricResult.keySet()) {
            for (LongRange newBucketRange : newBucketRanges) {
                if (newBucketRange.containsLong(oldTimeStamp)) {
                    newMetricResult.put(newBucketRange.getMinimumLong(), metricResult.get(oldTimeStamp));
                }
            }
        }

        return newMetricResult;
    }



    /**
     * Determine the timestamp for the beginning of the bucket into which the data for
     * a given timestamp would be aggregated.
     * @param initialTimeStamp A UTC timestamp for which some data is to be aggregated into a bucket
     * @param timeZone the boundary point for a bucket in UTC is dependent on what timezones the client
     *                 wants to view the aggregated data relative to.
     * @param bucketSize the length of a bucket, e.g. MONTHS.
     * @return a timestamp in UTC identifying the start of the bucket into which the data for the
     * given timestamp will be aggregated.
     */
    protected long determineInitialBucket(Long initialTimeStamp, TimeZone timeZone, TimeUnit bucketSize) {

        if(initialTimeStamp == null) {
            throw new IllegalArgumentException("Initial timestamp not specified");
        }

        if(timeZone == null) {
            throw new IllegalArgumentException("Timezone not specified");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucketsize not specified");
        }

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

        if (bucketSize == TimeUnit.YEARS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
        } else if (bucketSize == TimeUnit.MONTHS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
        } else if (bucketSize == TimeUnit.WEEKS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR));
            bucketStartCal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (bucketSize == TimeUnit.DAYS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
        } else if (bucketSize == TimeUnit.HOURS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
            bucketStartCal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY));
        } else if (bucketSize == TimeUnit.MINUTES) {
            //
            // Note: TimeUnit MINUTES should not be used here. Pre-aggregation is only needed
            // applicable for timeunits larger than MINUTES.
            //
            throw new IllegalArgumentException("TimeUnit MINUTES not supported for pre-aggregration.");
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit " + bucketSize);
        }

        return bucketStartCal.getTimeInMillis();
    }



    /**
     * Calculate the end time of a bucket given its start time and bucket size
     * @param bucketStart
     * @param bucketSize
     * @param timeZone
     * @return the end time of a bucket
     */
    public long getBucketEndTime(Long bucketStart, TimeUnit bucketSize, TimeZone timeZone) {

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucket size not specified");
        }

        if(bucketStart == null) {
            throw new IllegalArgumentException("Bucket start not specified");
        }

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);
        cal.setTimeInMillis(bucketStart);

        int fieldToStepForward;

        if(bucketSize == TimeUnit.YEARS) {
            fieldToStepForward = Calendar.YEAR;
        } else if(bucketSize == TimeUnit.MONTHS) {
            fieldToStepForward = Calendar.MONTH;
        } else if(bucketSize == TimeUnit.WEEKS) {
            fieldToStepForward = Calendar.WEEK_OF_YEAR;
        } else if(bucketSize == TimeUnit.DAYS) {
            fieldToStepForward = Calendar.DATE;
        } else if(bucketSize == TimeUnit.HOURS) {
            fieldToStepForward = Calendar.HOUR;
        } else if(bucketSize == TimeUnit.MINUTES) {
            fieldToStepForward = Calendar.MINUTE;
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit for bucketsize: " + bucketSize);
        }

        cal.add(fieldToStepForward, 1);
        return (cal.getTimeInMillis() - 1L);
    }

    private void logTimeStampAdjustments(Map<Long, Long> metricResult, Map<Long, Long> newMetricResult) {

        StringBuilder oldTimeStamps = new StringBuilder();
        for(long oldTimeStamp : metricResult.keySet()) {
            if(oldTimeStamps.length() > 0) {
                oldTimeStamps.append(", ");
            }
            oldTimeStamps.append(QueryLoggingUtil.getUTCFormat(oldTimeStamp) + "(" + oldTimeStamp  + ")");
        }

        logger.debug("Old bucket boundaries: [" + oldTimeStamps + "]");

        StringBuilder newTimeStamps = new StringBuilder();
        for(long newTimeStamp : newMetricResult.keySet()) {
            if(newTimeStamps.length() > 0) {
                newTimeStamps.append(", ");
            }
            newTimeStamps.append(QueryLoggingUtil.getUTCFormat(newTimeStamp) + "(" + newTimeStamp  + ")");
        }

        logger.debug("New bucket boundaries: [" + newTimeStamps + "]");
    }

}
