package com.petpal.tracking.web.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/8/14.
 */
public class TrackingDataUpload {

    private Map<String, TreeMap<Long, Long>> longMetrics;
    private Map<String, TreeMap<Long, Double>> doubleMetrics;
    private Map<String, TreeMap<Long, String>> stringMetrics;

    private Map<String, TreeMap> metricsData;

    public TrackingDataUpload() {
        longMetrics = new HashMap<String, TreeMap<Long, Long>>();
        doubleMetrics = new HashMap<String, TreeMap<Long, Double>>();
        stringMetrics = new HashMap<String, TreeMap<Long, String>>();
    }

    // This is called during deserialization of the request

    public void setLongMetrics(Map<String, TreeMap<Long, Long>> longMetrics) {
        this.longMetrics = longMetrics;
    }

    public void setDoubleMetrics(Map<String, TreeMap<Long, Double>> doubleMetrics) {
        this.doubleMetrics = doubleMetrics;
    }

    public void setStringMetrics(Map<String, TreeMap<Long, String>> stringMetrics) {
        this.stringMetrics = stringMetrics;
    }

    // This is called during validation and also during creation of TrackingData object

    public Map<String, TreeMap> getMetricsData() {

        if(metricsData == null) {

            metricsData = new HashMap<String, TreeMap>(
                    longMetrics.size() + doubleMetrics.size() + stringMetrics.size());

            for(String longMetric : longMetrics.keySet()) {
                metricsData.put(longMetric, new TreeMap());
                metricsData.get(longMetric).putAll(longMetrics.get(longMetric));
            }

            for(String doubleMetric : doubleMetrics.keySet()) {
                metricsData.put(doubleMetric, new TreeMap());
                metricsData.get(doubleMetric).putAll(doubleMetrics.get(doubleMetric));
            }

            for(String stringMetric : stringMetrics.keySet()) {
                metricsData.put(stringMetric, new TreeMap());
                metricsData.get(stringMetric).putAll(stringMetrics.get(stringMetric));
            }
        }

        return metricsData;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TrackingDataUpload{");
        sb.append("longMetrics: ").append(longMetrics.size()).append(" datapoints");
        sb.append(", doubleMetrics: ").append(doubleMetrics.size()).append(" datapoints");
        sb.append(", stringMetrics: ").append(stringMetrics.size()).append(" datapoints");
        sb.append('}');
        return sb.toString();
    }
}
