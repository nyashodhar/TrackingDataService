package com.petpal.tracking.service;

import com.petpal.tracking.web.controllers.AggregationLevel;

import java.lang.reflect.Type;

/**
 * Created by per on 12/9/14.
 */
public class TrackingMetricConfig {

    private String name;
    private Type aggregationDataType;
    private Type rawDataType;
    private Aggregation aggregation;
    private String unaggregatedSeriesName;

    public TrackingMetricConfig(
            String name,
            Type rawDataType,
            Type aggregationDataType,
            Aggregation aggregation) {

        this.name = name;
        this.rawDataType = rawDataType;
        this.aggregationDataType = aggregationDataType;
        this.aggregation = aggregation;
        this.unaggregatedSeriesName = name + "_RAW";

        if(aggregation == Aggregation.AVERAGE) {
            if(aggregationDataType != Double.class) {
                throw new IllegalArgumentException("Metric " + name + " has aggregation " +
                        aggregation + " but data type (" + aggregationDataType + " is not Double");
            }
        }
    }

    public String getName() {
        return name;
    }

    public Type getRawDataType() {
        return rawDataType;
    }

    public Type getAggregationDataType() {
        return aggregationDataType;
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
        sb.append(", aggregationDataType=").append(aggregationDataType);
        sb.append(", aggregation=").append(aggregation);
        sb.append('}');
        return sb.toString();
    }
}
