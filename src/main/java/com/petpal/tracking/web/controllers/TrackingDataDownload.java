package com.petpal.tracking.web.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/11/14.
 */
public class TrackingDataDownload {

    private Map<String, TreeMap> longMetrics;
    private Map<String, TreeMap> doubleMetrics;
    private Map<String, TreeMap> stringMetrics;

    public TrackingDataDownload() {
        longMetrics = new HashMap<String, TreeMap>();
        doubleMetrics = new HashMap<String, TreeMap>();
        stringMetrics = new HashMap<String, TreeMap>();
    }

    public Map<String, TreeMap> getLongMetrics() {
        return longMetrics;
    }

    public void setLongMetrics(Map<String, TreeMap> longMetrics) {
        this.longMetrics = longMetrics;
    }

    public Map<String, TreeMap> getDoubleMetrics() {
        return doubleMetrics;
    }

    public void setDoubleMetrics(Map<String, TreeMap> doubleMetrics) {
        this.doubleMetrics = doubleMetrics;
    }

    public Map<String, TreeMap> getStringMetrics() {
        return stringMetrics;
    }

    public void setStringMetrics(Map<String, TreeMap> stringMetrics) {
        this.stringMetrics = stringMetrics;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TrackingDataDownload{");
        sb.append("longMetrics: ").append(longMetrics.size()).append(" datapoints");
        sb.append(", doubleMetrics: ").append(doubleMetrics.size()).append(" datapoints");
        sb.append(", stringMetrics: ").append(stringMetrics.size()).append(" datapoints");
        sb.append('}');
        return sb.toString();
    }
}
