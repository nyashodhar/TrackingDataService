package com.petpal.tracking.web.controllers;

import com.petpal.tracking.service.TrackingDataService;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import com.petpal.tracking.web.editors.DateEditor;
import com.petpal.tracking.web.editors.TimeUnitEditor;
import com.petpal.tracking.web.editors.TrackingMetricsSet;
import com.petpal.tracking.web.editors.TrackingMetricsSetEditor;
import com.petpal.tracking.web.validators.util.AggregatedQueryUtil;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


/**
 * Controller to inject time data into and extract data
 * from the time series database.
 *
 * Created by per on 10/30/14.
 */
@Controller
public class TrackingDataController {

    private Logger logger = Logger.getLogger(this.getClass());

    @Value("${trackingService.defaultAggregatorTimeZoneID}")
    private String defaultAggregationTimeZoneID;

    @Autowired
    @Qualifier("trackingDataService")
    private TrackingDataService trackingDataService;

    /**
     * Store metrics in the time series database for a given device.
     * @param deviceId identified the device from which the tracking data originates
     * @param aggregationTimeZone the timezone the data should be aggregated for
     * @param trackingData the tracking data to be inserted into the tracking data store.
     */
    @RequestMapping(value="/tracking/device/{deviceId}", method=RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    public void saveTrackingDataForDevice(
            @PathVariable String deviceId,
            @RequestParam(value="aggregationTimeZone", required=false) TimeZone aggregationTimeZone,
            @RequestBody TrackingData trackingData) {

        StringBuilder str = new StringBuilder();
        str.append("saveTrackingDataForDevice(): deviceId: ").append(deviceId);
        str.append(", aggregationTimeZone = ");

        if(aggregationTimeZone != null) {
            str.append(aggregationTimeZone.getID());
        } else {
            str.append(aggregationTimeZone);
        }

        str.append(", trackingData = ").append(trackingData);
        logger.info(str);

        Map<TimeSeriesTag, String> tags = new HashMap<TimeSeriesTag, String>();
        tags.put(TimeSeriesTag.TRACKINGDEVICE, deviceId);

        if(aggregationTimeZone == null) {
            aggregationTimeZone = TimeZone.getTimeZone(defaultAggregationTimeZoneID);
        }

        trackingDataService.storeTrackingData(tags, trackingData, aggregationTimeZone);
    }


    /**
     * Get time series data for a specified set of tracking data metrics for a given device id.
     *
     * The metrics will be queries from aggregated time timeseries, the aggregated time series
     * that the actual query will run against is determined by the combination of tracking metrics
     * and the result buckets specified.
     *
     * This API call uses absolute timing and allows the client to control the boundaries of
     * each bucket.
     *
     * For example, if a call is made to get data from midnight 3 days ago in buckets spanning 1 day,
     * then the first bucket will span 24 hours starting midnight 3 days ago. Bucket 2 will contain data
     * spanning 24 hrs from midnight two days ago, etc.
     *
     * @param deviceId the device id
     * @param resultBucketSize the time unit used to determine bucket size for result.
     * @param startYear
     * @param startMonth
     * @param startWeek
     * @param startDay
     * @param startHour
     * @param trackingMetricsSet the metrics the clients wants to query for. If null, all known
     *                           metrics will be queried for.
     * @param verboseResponse if set to true, the response will include buckets for which
     *                       values were found for the metric. If null, it will default to 'false'
     *                       and a sparse response will be sent.
     * @return Tracking data results grouped by metric and in bucket of the specified size.
     */
    @RequestMapping(value="/metrics/device/{deviceId}/aggregate/{resultBucketSize}", method=RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public @ResponseBody Map<TrackingMetric, Map<Long, Long>> getAggregatedMetricsForDevice(
            @PathVariable(value="deviceId") String deviceId,
            @PathVariable(value="resultBucketSize") TimeUnit resultBucketSize,
            @RequestParam(value="startYear", required=true) Integer startYear,
            @RequestParam(value="startMonth", required=false) Integer startMonth,
            @RequestParam(value="startWeek", required=false) Integer startWeek,
            @RequestParam(value="startDay", required=false) Integer startDay,
            @RequestParam(value="startHour", required=false) Integer startHour,
            @RequestParam(value="bucketsToFetch", required=false) Integer bucketsToFetch,
            @RequestParam(value="trackingMetrics", required=false) TrackingMetricsSet trackingMetricsSet,
            @RequestParam(value="verboseResponse", required=false) Boolean verboseResponse,
            @RequestParam(value="aggregationTimeZone", required=false) TimeZone aggregationTimeZone) {

        StringBuilder str = new StringBuilder();
        str.append("getAggregatedMetricsForDevice(): deviceId = ").append(deviceId);
        str.append(", resultBucketSize = ").append(resultBucketSize);
        str.append(", startYear = ").append(startYear);
        str.append(", startMonth = ").append(startMonth);
        str.append(", startWeek = ").append(startWeek);
        str.append(", startDay = ").append(startDay);
        str.append(", startHour = ").append(startHour);
        str.append(", bucketsToFetch = ").append(bucketsToFetch);
        str.append(", trackingMetricsSet = ").append(trackingMetricsSet);
        str.append(", verboseResponse = ").append(verboseResponse);
        str.append(", aggregationTimeZone = ");

        if(aggregationTimeZone != null) {
            str.append(aggregationTimeZone.getID());
        } else {
            str.append(aggregationTimeZone);
        }

        logger.info(str);

        Map<TimeSeriesTag, String> tags = new HashMap<TimeSeriesTag, String>();
        tags.put(TimeSeriesTag.TRACKINGDEVICE, deviceId);

        //
        // The aggregation timezone is needed to perform a reverse shift of the aggregated data.
        // When the aggregated data was originally stored, it was stored into buckets
        // series whose timestamps are shifted relative to the UTZ timezone. This is necessary to make the
        // bucket boundaries deterministic in the case users change their timezones.
        // At that point we would not be able to query for their aggregated data anymore.
        //

        if(aggregationTimeZone == null) {
            aggregationTimeZone = TimeZone.getTimeZone(defaultAggregationTimeZoneID);
        }

        Long utcBegin = AggregatedQueryUtil.calculateUTCBegin(
                startYear, startMonth, startWeek, startDay, startHour, resultBucketSize, aggregationTimeZone);
        Long utcEnd = AggregatedQueryUtil.calculateUTCEnd(
                utcBegin, resultBucketSize, bucketsToFetch, aggregationTimeZone);

        //
        // If the request is itemizing the metrics to query for, only pass those along.
        // Otherwise, query for all known metrics
        //

        List<TrackingMetric> trackingMetricsParam = new ArrayList<TrackingMetric>();
        if(CollectionUtils.isEmpty(trackingMetricsSet)) {
            trackingMetricsParam.addAll(TrackingMetric.getAllTrackingMetrics());
        } else {
            trackingMetricsParam.addAll(trackingMetricsSet);
        }

        boolean createVerboseResponse = (verboseResponse == null) ? false : verboseResponse;

        Map<TrackingMetric, Map<Long, Long>> metricResults = trackingDataService.getAggregatedTimeSeries(
                tags, trackingMetricsParam, utcBegin, utcEnd, resultBucketSize, aggregationTimeZone, 1, createVerboseResponse);

        logger.info("getTrackingMetricsForDevice(): Results: " + metricResults);
        return metricResults;
    }


    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(TrackingMetricsSet.class, new TrackingMetricsSetEditor());
        binder.registerCustomEditor(Date.class, new DateEditor());
        binder.registerCustomEditor(TimeUnit.class, new TimeUnitEditor());
    }

}
