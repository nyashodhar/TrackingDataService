package com.petpal.tracking.web.controllers;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.TrackingDataService;
import com.petpal.tracking.service.TrackingMetric;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import com.petpal.tracking.web.editors.DateEditor;
import com.petpal.tracking.web.editors.TrackingMetricsSet;
import com.petpal.tracking.web.editors.TrackingMetricsSetEditor;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
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


/**
 * Controller to inject time data into and extract data
 * from the time series database.
 *
 * Created by per on 10/30/14.
 */
@Controller
public class TrackingDataController {

    private Logger logger = Logger.getLogger(this.getClass());

    @Autowired
    private TrackingDataService trackingService;

    /**
     * Store metrics in the time series database for a given device.
     *
     * CURL EXAMPLE:
     * curl -v -X POST localhost:9000/tracking/device/lkjslfjssdddss -H "Accept: application/json" -H "Content-Type: application/json" -d '{"walkingData":{"23423424523523":123,"23423424523700":125}}'
     */
    @RequestMapping(value="/tracking/device/{deviceId}", method=RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    public void saveTrackingDataForDevice(
            @PathVariable String deviceId,
            @RequestBody TrackingData trackingData) {

        logger.info("saveTrackingDataForDevice(): deviceId: " + deviceId);
        logger.info("saveTrackingDataForDevice(): tracking data: " + trackingData);

        Map<TimeSeriesTag, String> tags = new HashMap<TimeSeriesTag, String>();
        tags.put(TimeSeriesTag.TRACKINGDEVICE, deviceId);

        trackingService.storeTrackingData(tags, trackingData);
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
     * CURL EXAMPLE:
     *  utcBegin: "May 1st 2014 PDT, Midnight" (UTC millis - 1398927600265)
     *  utcEnd: "Oct 31st 2014 PDT, Midnight" (UTC millis - 1414738800266)
     * curl -v -X GET "http://localhost:9000/metrics/device/263e6c54-69c9-45f5-853c-b5f4420ceb5e?utcBegin=1398927600265&utcEnd=1414738800266&resultBucketSize=MONTHS&resultBucketMultiplier=1&trackingMetrics=walkingsteps,runningsteps" -H "Accept: application/json" -H "Content-Type: application/json"
     *
     * @param deviceId the device id
     * @param utcBegin utc time stamp for the start of the query interval. Can not be null.
     * @param utcBegin utc time stamp for the end of the query interval. If not, the end will be 'now'.
     * @param resultBucketSize the time unit used to determine bucket size for result.
     * @param resultBucketMultiplier multiplier for bucket size for result.
     * @param trackingMetricsSet the metrics the clients wants to query for. If null, all known
     *                           metrics will be queries for.
     * @param verboseResponse if set to true, the response will include buckets for which
     *                       values were found for the metric. If null, it will default to 'false'
     *                       and a sparse response will be sent.
     * @return Tracking data results grouped by metric and in bucket of the specified size.
     */
    @RequestMapping(value="/metrics/device/{deviceId}", method=RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public @ResponseBody Map<TrackingMetric, Map<Long, Long>> getTrackingMetricsForDevice(
            @PathVariable String deviceId,
            @RequestParam(value="utcBegin", required=true) Long utcBegin,
            @RequestParam(value="utcEnd", required=false) Long utcEnd,
            @RequestParam(value="resultBucketSize", required=true) TimeUnit resultBucketSize,
            @RequestParam(value="resultBucketMultiplier", required=true) Integer resultBucketMultiplier,
            @RequestParam(value="trackingMetrics", required=false) TrackingMetricsSet trackingMetricsSet,
            @RequestParam(value="verboseResponse", required=false) Boolean verboseResponse) {

        logger.info("getTrackingMetricsForDevice(): deviceId = " + deviceId);
        logger.info("getTrackingMetricsForDevice(): utcBegin = " + utcBegin + " (" + new Date(utcBegin) + ")");
        logger.info("getTrackingMetricsForDevice(): utcEnd = " + utcEnd);
        logger.info("getTrackingMetricsForDevice(): resultBucketSize = " + resultBucketSize);
        logger.info("getTrackingMetricsForDevice(): resultBucketMultiplier = " + resultBucketMultiplier);
        logger.info("getTrackingMetricsForDevice(): trackingMetricsSet = " + trackingMetricsSet);
        logger.info("getTrackingMetricsForDevice(): verboseResponse = " + verboseResponse);

        Map<TimeSeriesTag, String> tags = new HashMap<TimeSeriesTag, String>();
        tags.put(TimeSeriesTag.TRACKINGDEVICE, deviceId);

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

        Map<TrackingMetric, Map<Long, Long>> metricResults = trackingService.getAggregatedTimeSeriesData(
                tags, trackingMetricsParam, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier, createVerboseResponse);

        logger.info("getTrackingMetricsForDevice(): Results: " + metricResults);
        return metricResults;
    }


    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(TrackingMetricsSet.class, new TrackingMetricsSetEditor());
        binder.registerCustomEditor(Date.class, new DateEditor());
    }

}
