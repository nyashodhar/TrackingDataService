package com.petpal.tracking.web.controllers;

import com.petpal.tracking.service.TrackingDataService;
import com.petpal.tracking.service.TrackingMetricConfig;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.editors.AggregationLevelEditor;
import com.petpal.tracking.web.editors.DateEditor;
import com.petpal.tracking.web.editors.util.TrackingDataUploadUtil;
import com.petpal.tracking.web.validators.util.AggregatedQueryUtil;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;
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
import java.util.Set;
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

    @Autowired
    @Qualifier("trackingDataUploadValidator")
    private Validator trackingDataUploadValidator;

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    /**
     * Client example:
     *
     *    curl -v -k -X POST -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:9000/tracking/device/5f17e09e-221b-4b82-a033-9318d3f09aaa?aggregationTimeZone=PST" -d '{"longMetrics":{"WALKINGSTEPS":{"1401346800000":3,"1404284400000":2,"1408690800000":233}}}'
     *
     * @param deviceId
     * @param aggregationTimeZone
     * @param trackingDataUpload
     */
    @RequestMapping(value="/tracking/device/{deviceId}", method=RequestMethod.POST)
    @ResponseStatus( HttpStatus.NO_CONTENT )
    public void saveTrackingDataForDevice(
            @PathVariable String deviceId,
            @RequestParam(value="aggregationTimeZone", required=false) TimeZone aggregationTimeZone,
            @RequestBody @Validated TrackingDataUpload trackingDataUpload) {

        StringBuilder str = new StringBuilder();
        str.append("saveTrackingDataForDevice(): deviceId: ").append(deviceId);
        str.append(", aggregationTimeZone = ");

        if(aggregationTimeZone != null) {
            str.append(aggregationTimeZone.getID());
        } else {
            str.append(aggregationTimeZone);
        }

        str.append(", trackingDataUpload = ").append(trackingDataUpload);
        logger.info(str);

        Map<TrackingTag, String> tags = new HashMap<TrackingTag, String>();
        tags.put(TrackingTag.TRACKINGDEVICE, deviceId);

        if(aggregationTimeZone == null) {
            aggregationTimeZone = TimeZone.getTimeZone(defaultAggregationTimeZoneID);
        }

        TrackingData trackingData = TrackingDataUploadUtil.createTrackingDataFromUploadRequest(
                trackingDataUpload, trackingMetricsConfig);

        trackingDataService.storeTrackingData(tags, trackingData, aggregationTimeZone);
    }


    /**
     * Get time series data for a specified set of tracking data metrics for a given device id.
     *
     * The metrics will be queries from aggregated time timeseries, the aggregated time series
     * that the actual query will run against is determined by the combination of tracking metrics
     * and the result buckets specified.
     *
     * Client example:
     *
     *    curl -v -k -X GET -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:9000/metrics/device/5f17e09e-221b-4b82-a033-9318d3f09aaa/aggregate/YEARS?startYear=2014"
     *
     * @param deviceId the device id
     * @param aggregationLevel the resolution of the aggregated time series to query from.
     * @param startYear
     * @param startMonth
     * @param startWeek
     * @param startDay
     * @param startHour
     * @param bucketsToFetch
     * @param trackingMetricsSet the metrics the clients wants to query for. If null, all known
     *                           metrics will be queried for.
     * @param aggregationTimeZone
     * @return Tracking data results grouped by metric and in bucket of the specified size.
     */
    @RequestMapping(value="/metrics/device/{deviceId}/aggregate/{aggregationLevel}", method=RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public @ResponseBody TrackingDataDownload getAggregatedMetricsForDevice(
            @PathVariable(value="deviceId") String deviceId,
            @PathVariable(value="aggregationLevel") AggregationLevel aggregationLevel,
            @RequestParam(value="startYear", required=true) Integer startYear,
            @RequestParam(value="startMonth", required=false) Integer startMonth,
            @RequestParam(value="startWeek", required=false) Integer startWeek,
            @RequestParam(value="startDay", required=false) Integer startDay,
            @RequestParam(value="startHour", required=false) Integer startHour,
            @RequestParam(value="bucketsToFetch", required=false) Integer bucketsToFetch,
            @RequestParam(value="trackingMetrics", required=false) Set<String> trackingMetricsSet,
            @RequestParam(value="aggregationTimeZone", required=false) TimeZone aggregationTimeZone) {

        StringBuilder str = new StringBuilder();
        str.append("getAggregatedMetricsForDevice(): deviceId = ").append(deviceId);
        str.append(", aggregationLevel = ").append(aggregationLevel);
        str.append(", startYear = ").append(startYear);
        str.append(", startMonth = ").append(startMonth);
        str.append(", startWeek = ").append(startWeek);
        str.append(", startDay = ").append(startDay);
        str.append(", startHour = ").append(startHour);
        str.append(", bucketsToFetch = ").append(bucketsToFetch);
        str.append(", trackingMetricsSet = ").append(trackingMetricsSet);
        str.append(", aggregationTimeZone = ");

        if(aggregationTimeZone != null) {
            str.append(aggregationTimeZone.getID());
        } else {
            str.append(aggregationTimeZone);
        }

        logger.info(str);

        Map<TrackingTag, String> tags = new HashMap<TrackingTag, String>();
        tags.put(TrackingTag.TRACKINGDEVICE, deviceId);

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
                startYear, startMonth, startWeek, startDay, startHour, aggregationLevel, aggregationTimeZone);

        //
        // Hack: There is an off by 1 problem with 'buckets to fetch' due to the way the
        // bucket end is being calculated as being 1 ms prior to the beginning of the next
        // bucket. Hence, when specifying a single bucket, the end time stamp will also
        // belong to the first bucket. To get two buckets, we work around this here by
        // increasing the value for 'bucketsToFetch' by 1.
        //

        if(bucketsToFetch != null) {
            bucketsToFetch = new Integer(bucketsToFetch.intValue() + 1);
        }

        Long utcEnd = AggregatedQueryUtil.calculateUTCEnd(
                utcBegin, aggregationLevel, bucketsToFetch, aggregationTimeZone);

        //
        // If the request is itemizing the metrics to query for, only pass those along.
        // Otherwise, query for all known metrics
        //

        List<TrackingMetricConfig> trackingMetricConfigs = new ArrayList<TrackingMetricConfig>();
        if(CollectionUtils.isEmpty(trackingMetricsSet)) {
            trackingMetricConfigs.addAll(trackingMetricsConfig.getAllMetrics().values());
        } else {
            for(String metricName : trackingMetricsSet) {
                trackingMetricConfigs.add(trackingMetricsConfig.getTrackingMetric(metricName));
            }
        }

        TrackingDataDownload trackingDataDownload = trackingDataService.getAggregatedTimeSeries(
                tags, trackingMetricConfigs, utcBegin, utcEnd, aggregationLevel, aggregationTimeZone, 1);

        logger.info("getTrackingMetricsForDevice(): Result: " + trackingDataDownload);
        return trackingDataDownload;
    }


    @InitBinder
    public void initBinder(WebDataBinder binder) {

        // Editors
        binder.registerCustomEditor(Date.class, new DateEditor());
        binder.registerCustomEditor(AggregationLevel.class, new AggregationLevelEditor());
    }

    @InitBinder("trackingDataUpload")
    public void initTrackingDataBinder(WebDataBinder webDataBinder) {
        webDataBinder.addValidators(trackingDataUploadValidator);
    }

}
