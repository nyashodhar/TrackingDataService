package com.petpal.tracking.web.controllers;

import com.petpal.tracking.service.TrackingMetricConfig;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/11/14.
 */
public class TrackingData {

    private TrackingDataUpload trackingDataUpload;
    private Map<TrackingMetricConfig, TreeMap> metricConfigAndData;

    public TrackingData(
            TrackingDataUpload trackingDataUpload,
            Map<TrackingMetricConfig, TreeMap> metricConfigAndData) {
        this.trackingDataUpload = trackingDataUpload;
        this.metricConfigAndData = metricConfigAndData;
    }

    public Map<TrackingMetricConfig, TreeMap> getMetricConfigAndData() {
        return metricConfigAndData;
    }
}
