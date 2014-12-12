package com.petpal.tracking.service;

import com.petpal.tracking.web.controllers.AggregationLevel;

import java.lang.reflect.Type;

/**
 * Created by per on 12/9/14.
 */
public class TrackingMetricConfig {

    private String name;
    private Type dataType;
    private Aggregation aggregation;
    private String unaggregatedSeriesName;

    public TrackingMetricConfig(String name, Type dataType, Aggregation aggregation) {
        this.name = name;
        this.dataType = dataType;
        this.aggregation = aggregation;
        this.unaggregatedSeriesName = name + "_RAW";
    }

    public String getName() {
        return name;
    }

    public Type getDataType() {
        return dataType;
    }

    public Aggregation getAggregation() {
        return aggregation;
    }

    public String getUnaggregatedSeriesName() {
        return unaggregatedSeriesName;
    }

    public String getAggregatedSeriesName(AggregationLevel aggregationLevel) {
        return name + "_" + aggregationLevel.toString().toUpperCase();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TrackingMetric{");
        sb.append("name='").append(name).append('\'');
        sb.append(", dataType=").append(dataType);
        sb.append(", aggregation=").append(aggregation);
        sb.append('}');
        return sb.toString();
    }
}
