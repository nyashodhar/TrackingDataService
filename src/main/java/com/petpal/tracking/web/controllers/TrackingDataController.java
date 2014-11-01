package com.petpal.tracking.web.controllers;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.TrackingDataService;
import com.petpal.tracking.service.TrackingMetric;
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

    @RequestMapping(value="/metric", method= RequestMethod.GET)
    public @ResponseBody
    String getMetric(@RequestParam(value="name", required=false, defaultValue="Stranger") String metricName) {
        return "getMetric() org.petpal.data.hello";
    }

    /**
     * Store metrics in the time series database.
     *
     * CURL EXAMPLE:
     * curl -v -X POST localhost:9000/tracking -H "Accept: application/json" -H "Content-Type: application/json" -d '{"walkingData":{"23423424523523":123,"23423424523700":125}}'
     */
    @RequestMapping(value="/tracking", method=RequestMethod.POST)
    @ResponseStatus( HttpStatus.CREATED )
    public @ResponseBody String saveTrackingData(@RequestBody TrackingData trackingData) {
        System.out.println("saveTrackingData(): Received tracking data " + trackingData);

        trackingService.storeTrackingData(trackingData);
        return "saveTrackingData() org.petpal.data.hello";
    }


    /**
     * Get time series data for a specified set of tracking data metrics for a given device id
     *
     * Note that this controller uses absolute timing, which allows the client to control the
     * boundaries of each bucket.
     *
     * For example, if a call is made to get data from midnight 3 days ago in buckets spanning 1 day,
     * then the first bucket will span 24 hours starting midnight 3 days ago. Bucket 2 will contain data
     * spanning 24 hrs from midnight two days ago, etc.
     *
     * CURL EXAMPLE:
     *  utcBegin: "May 1st 2014 PDT, Midnight" (UTC millis - 1398927600265)
     *  utcEnd: "Oct 31st 2014 PDT, Midnight" (UTC millis - 1414738800266)
     * curl -v -X GET "http://localhost:9000/metrics/absolute/device/263e6c54-69c9-45f5-853c-b5f4420ceb5e?utcBegin=1398927600265&utcEnd=1414738800266&resultBucketSize=MONTHS&resultBucketMultiplier=1&trackingMetrics=walkingsteps,runningsteps" -H "Accept: application/json" -H "Content-Type: application/json"
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
    @RequestMapping(value="/metrics/absolute/device/{deviceId}", method=RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    public @ResponseBody Map<TrackingMetric, Map<Long, Long>> getMetricsForDeviceAbsoluteTiming(
            @PathVariable String deviceId,
            @RequestParam(value="utcBegin", required=true) Long utcBegin,
            @RequestParam(value="utcEnd", required=false) Long utcEnd,
            @RequestParam(value="resultBucketSize", required=true) TimeUnit resultBucketSize,
            @RequestParam(value="resultBucketMultiplier", required=true) Integer resultBucketMultiplier,
            @RequestParam(value="trackingMetrics", required=false) TrackingMetricsSet trackingMetricsSet,
            @RequestParam(value="verboseResponse", required=false) Boolean verboseResponse) {

        logger.info("getMetricsForDeviceRelativeTiming(): deviceId = " + deviceId);
        logger.info("getMetricsForDeviceRelativeTiming(): utcBegin = " + utcBegin);
        logger.info("getMetricsForDeviceRelativeTiming(): utcEnd = " + utcEnd);
        logger.info("getMetricsForDeviceRelativeTiming(): resultBucketSize = " + resultBucketSize);
        logger.info("getMetricsForDeviceRelativeTiming(): resultBucketMultiplier = " + resultBucketMultiplier);
        logger.info("getMetricsForDeviceRelativeTiming(): trackingMetricsSet = " + trackingMetricsSet);
        logger.info("getMetricsForDeviceRelativeTiming(): verboseResponse = " + verboseResponse);

        Map<String, String> tags = new HashMap<String, String>();
        //tags.put(TrackingDataService.TAG_TRACKED_ENTITY, "c45c4cd8-06fd-41be-aa0c-76a5418d3021");
        tags.put(TrackingDataService.TAG_TRACKING_DEVICE, deviceId);

        //
        // If the request is itemizing the metrics to query for, only pass those along.
        // Otherwise, query for all known metrics
        //

        List<TrackingMetric> trackingMetricsParam = new ArrayList<TrackingMetric>();
        if(CollectionUtils.isEmpty(trackingMetricsSet)) {
            trackingMetricsParam.add(TrackingMetric.WALKINGSTEPS);
            trackingMetricsParam.add(TrackingMetric.RUNNINGSTEPS);
            trackingMetricsParam.add(TrackingMetric.RESTINGSECONDS);
            trackingMetricsParam.add(TrackingMetric.SLEEPINGSECONDS);
        } else {
            trackingMetricsParam.addAll(trackingMetricsSet);
        }

        boolean createVerboseResponse = (verboseResponse == null) ? false : verboseResponse;

        Map<TrackingMetric, Map<Long, Long>> metricResults = trackingService.getMetricsForAbsoluteRange(
                tags, trackingMetricsParam, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier, createVerboseResponse);

        logger.info("getMetricsForDeviceAbsoluteTiming(): Results: " + metricResults);
        return metricResults;
    }


    /**
     * Get time series data for a specified set of tracking data metrics for a given device id
     *
     * Note that this controller uses RELATIVE timing and the results will not line up on
     * edges of time units.
     *
     * For example, if a call is made to get data for the last 3 days in buckets spanning 1 day,
     * it will return 3 buckets of data, but the data in the first bucket will span 24 hours from
     * the point in time that is 3x24 hours earlier than the current time.
     *
     * If the client wants results where the buckets align on certain boundaries (like midnight, or
     * top of the hour etc), the API call that uses absolute timing interval spec should be used.
     *
     * CURL EXAMPLE:
     * curl -v -X GET "http://localhost:9000/metrics/device/263e6c54-69c9-45f5-853c-b5f4420ceb5e?beginUnitsIntoPast=6&endUnitsIntoPast=3&queryIntervalTimeUnit=MONTHS&resultTimeUnit=MONTHS&resultBucketMultiplier=1&trackingMetrics=walkingsteps,runningsteps" -H "Accept: application/json" -H "Content-Type: application/json"
     *
     * @param deviceId the device id
     * @param beginUnitsIntoPast number of steps (multiplied with queryIntervalTimeUnit) into the past
     *                           to get to the start of the query interval. Can not be null.
     * @param endUnitsIntoPast number of steps (multiplied with queryIntervalTimeUnit) into the past
     *                         to get to the end of the query interval. If null, the query time interval
     *                         ends with 'now'.
     * @param queryIntervalTimeUnit the time unit used in query time interval calculation. E.g. MONTHS.
     * @param resultTimeUnit the time unit used to determine bucket size for result.
     * @param resultBucketMultiplier multiplier for bucket size for result.
     * @param trackingMetricsSet the metrics the clients wants to query for. If null, all known
     *                           metrics will be queries for.
     * @return Tracking data results grouped by metric and in bucket of the specified size.
     */
    @RequestMapping(value="/metrics/device/{deviceId}", method=RequestMethod.GET)
    @ResponseStatus( HttpStatus.OK )
    public @ResponseBody Map<TrackingMetric, Map<Long, Long>> getMetricsForDeviceRelativeTiming(
            @PathVariable String deviceId,
            @RequestParam(value="beginUnitsIntoPast", required=true) Integer beginUnitsIntoPast,
            @RequestParam(value="endUnitsIntoPast", required=false) Integer endUnitsIntoPast,
            @RequestParam(value="queryIntervalTimeUnit", required=true) TimeUnit queryIntervalTimeUnit,
            @RequestParam(value="resultTimeUnit", required=true) TimeUnit resultTimeUnit,
            @RequestParam(value="resultBucketMultiplier", required=true) Integer resultBucketMultiplier,
            @RequestParam(value="trackingMetrics", required=false) TrackingMetricsSet trackingMetricsSet) {

        logger.info("getMetricsForDeviceRelativeTiming(): deviceId = " + deviceId);
        logger.info("getMetricsForDeviceRelativeTiming(): beginUnitsIntoPast = " + beginUnitsIntoPast);
        logger.info("getMetricsForDeviceRelativeTiming(): endUnitsIntoPast = " + endUnitsIntoPast);
        logger.info("getMetricsForDeviceRelativeTiming(): queryIntervalTimeUnit = " + queryIntervalTimeUnit);
        logger.info("getMetricsForDeviceRelativeTiming(): resultTimeUnit = " + resultTimeUnit);
        logger.info("getMetricsForDeviceRelativeTiming(): resultBucketMultiplier = " + resultBucketMultiplier);
        logger.info("getMetricsForDeviceRelativeTiming(): trackingMetricsSet = " + trackingMetricsSet);

        //return new HashMap<TrackingMetric, Map<Long, Long>>();

        Map<String, String> tags = new HashMap<String, String>();
        //tags.put(TrackingDataService.TAG_TRACKED_ENTITY, "c45c4cd8-06fd-41be-aa0c-76a5418d3021");
        tags.put(TrackingDataService.TAG_TRACKING_DEVICE, deviceId);

        //
        // If the request is itemizing the metrics to query for, only pass those along.
        // Otherwise, query for all known metrics
        //

        List<TrackingMetric> trackingMetricsParam = new ArrayList<TrackingMetric>();
        if(CollectionUtils.isEmpty(trackingMetricsSet)) {
            trackingMetricsParam.add(TrackingMetric.WALKINGSTEPS);
            trackingMetricsParam.add(TrackingMetric.RUNNINGSTEPS);
            trackingMetricsParam.add(TrackingMetric.RESTINGSECONDS);
            trackingMetricsParam.add(TrackingMetric.SLEEPINGSECONDS);
        } else {
            trackingMetricsParam.addAll(trackingMetricsSet);
        }

        Map<TrackingMetric, Map<Long, Long>> metricResults = trackingService.getMetrics(
                tags, trackingMetricsParam, beginUnitsIntoPast, endUnitsIntoPast, queryIntervalTimeUnit, resultTimeUnit, resultBucketMultiplier);

        logger.info("getMetrics(): Results: " + metricResults);
        return metricResults;
    }

    @InitBinder
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(TrackingMetricsSet.class, new TrackingMetricsSetEditor());
        binder.registerCustomEditor(Date.class, new DateEditor());
    }

}
