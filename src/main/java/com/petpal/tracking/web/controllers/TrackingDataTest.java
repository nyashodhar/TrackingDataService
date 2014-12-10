package com.petpal.tracking.web.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/8/14.
 */
public class TrackingDataTest {

    private Map<String, TreeMap<Long, Long>> longMetrics;
    private Map<String, TreeMap<Long, Long>> doubleMetrics;
    private Map<String, TreeMap<Long, Long>> stringMetrics;

    public void setLongMetrics(Map<String, TreeMap<Long, Long>> longMetrics) {
        this.longMetrics = longMetrics;
    }

    public void setDoubleMetrics(Map<String, TreeMap<Long, Long>> doubleMetrics) {
        this.doubleMetrics = doubleMetrics;
    }

    public void setStringMetrics(Map<String, TreeMap<Long, Long>> stringMetrics) {
        this.stringMetrics = stringMetrics;
    }

    public Map<String, TreeMap<Long, Long>> getLongMetrics() {
        return longMetrics == null ? new HashMap<String, TreeMap<Long, Long>>() : longMetrics;
    }

    public Map<String, TreeMap<Long, Long>> getDoubleMetrics() {
        return doubleMetrics == null ? new HashMap<String, TreeMap<Long, Long>>() : doubleMetrics;
    }

    public Map<String, TreeMap<Long, Long>> getStringMetrics() {
        return stringMetrics == null ? new HashMap<String, TreeMap<Long, Long>>() : stringMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TrackingDataTest{");
        sb.append("longMetrics=").append("[").append(getMetricSummary(getLongMetrics())).append("]");
        sb.append(", doubleMetrics=").append("[").append(getMetricSummary(getDoubleMetrics())).append("]");
        sb.append(", stringMetrics=").append("[").append(getMetricSummary(getStringMetrics())).append("]");
        sb.append('}');
        return sb.toString();
    }

    private String getMetricSummary(Map metrics) {
        StringBuilder summary = new StringBuilder();
        for(Object key : metrics.keySet()) {
            if(summary.length() > 0) {
                summary.append(", ");
            }
            summary.append(key.toString() + ": " + metrics.keySet().size() + " datapoints");
        }
        return summary.toString();
    }

}
