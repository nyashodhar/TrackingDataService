package com.petpal.tracking.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 11/3/14.
 */
public class TestTrackingDataUpload {

    private Map<String, TreeMap<Long, Long>> longMetrics;
    private Map<String, TreeMap<Long, Double>> doubleMetrics;
    private Map<String, TreeMap<Long, String>> stringMetrics;

    public TestTrackingDataUpload() {
        longMetrics = new HashMap<String, TreeMap<Long, Long>>();
        doubleMetrics = new HashMap<String, TreeMap<Long, Double>>();
        stringMetrics = new HashMap<String, TreeMap<Long, String>>();
    }

    public void setLongMetrics(Map<String, TreeMap<Long, Long>> longMetrics) {
        this.longMetrics = longMetrics;
    }

    public void setDoubleMetrics(Map<String, TreeMap<Long, Double>> doubleMetrics) {
        this.doubleMetrics = doubleMetrics;
    }

    public void setStringMetrics(Map<String, TreeMap<Long, String>> stringMetrics) {
        this.stringMetrics = stringMetrics;
    }

    public Map<String, TreeMap<Long, Long>> getLongMetrics() {
        return longMetrics;
    }

    public Map<String, TreeMap<Long, Double>> getDoubleMetrics() {
        return doubleMetrics;
    }

    public Map<String, TreeMap<Long, String>> getStringMetrics() {
        return stringMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TestTrackingData{");
        sb.append("longMetrics: ").append(longMetrics.size()).append(" datapoints");
        sb.append(", doubleMetrics: ").append(doubleMetrics.size()).append(" datapoints");
        sb.append(", stringMetrics: ").append(stringMetrics.size()).append(" datapoints");
        sb.append('}');
        return sb.toString();
    }
}
