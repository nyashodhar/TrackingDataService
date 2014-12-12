package com.petpal.tracking.web.editors.util;

import com.petpal.tracking.service.TrackingMetricConfig;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingData;
import com.petpal.tracking.web.controllers.TrackingDataUpload;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/11/14.
 */
public class TrackingDataUploadUtil {

    public static TrackingData createTrackingDataFromUploadRequest(
            TrackingDataUpload trackingDataUpload, TrackingMetricsConfig trackingMetricsConfig) {

        Map<String, TreeMap> metricsData = trackingDataUpload.getMetricsData();

        Map<TrackingMetricConfig, TreeMap> metricConfigAndData =
                new HashMap<TrackingMetricConfig, TreeMap>(metricsData.size());

        for(String metricName : metricsData.keySet()) {

            TrackingMetricConfig trackingMetricConfig =
                    trackingMetricsConfig.getTrackingMetric(metricName);

            TreeMap dataPointsForMetric = metricsData.get(metricName);

            metricConfigAndData.put(trackingMetricConfig, dataPointsForMetric);
        }

        TrackingData trackingData = new TrackingData(trackingDataUpload, metricConfigAndData);
        return trackingData;
    }

}
