package com.petpal.tracking.service;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by per on 12/5/14.
 */
@Component("trackingMetricConfig")
public class TrackingMetricsConfig {

    private static final String CONFIG_FILE = "tracking-metrics.properties";

    private static final String DATA_TYPE_LONG = "Long";
    private static final String DATA_TYPE_DOUBLE = "Double";
    private static final String DATA_TYPE_STRING = "String";

    private Properties trackingMetricsProperties;

    private Map<String, TrackingMetricConfig> trackingMetrics;

    @PostConstruct
    public void initialize() throws IOException {

        Resource resource = new ClassPathResource("/" + CONFIG_FILE);
        trackingMetricsProperties = PropertiesLoaderUtils.loadProperties(resource);
        trackingMetrics = new HashMap<String, TrackingMetricConfig>();

        int i=1;
        boolean lookForMetric = true;

        while(lookForMetric) {

            String metricName = trackingMetricsProperties.getProperty("metric." + i + ".name");
            if(StringUtils.isEmpty(metricName)) {
                lookForMetric = false;
                if(trackingMetrics.isEmpty()) {
                    throw new IllegalStateException("No metrics found in " + CONFIG_FILE);
                }
                continue;
            }

            Type dataType = getDataTypeFromProperties(i, trackingMetricsProperties, metricName, "aggregated");
            Aggregation aggregation = getAggregationFromProperties(i, trackingMetricsProperties, metricName);

            TrackingMetricConfig trackingMetric = new TrackingMetricConfig(metricName, dataType, aggregation);

            if(trackingMetrics.get(trackingMetric.getName()) != null) {
                throw new IllegalArgumentException("Metric " + trackingMetric.getName() +
                        " is repeated in the config file " + CONFIG_FILE);
            }

            trackingMetrics.put(trackingMetric.getName(), trackingMetric);
            i++;
        }
    }

    public Map<String, TrackingMetricConfig> getAllMetrics() {
        Map<String, TrackingMetricConfig> configCopy = new HashMap<String, TrackingMetricConfig>(trackingMetrics.size());
        configCopy.putAll(trackingMetrics);
        return configCopy;
    }

    public TrackingMetricConfig getTrackingMetric(String metricName) {
        if(trackingMetrics.containsKey(metricName)) {
            return trackingMetrics.get(metricName);
        } else {
            throw new IllegalArgumentException("No tracking metric matches metric name " + metricName);
        }
    }

    protected Aggregation getAggregationFromProperties(
            int metricIndex, Properties trackingMetricsProperties, String metricName) {

        String aggregationStr = trackingMetricsProperties.getProperty("metric." + metricIndex + ".aggregation");
        Assert.isTrue(!StringUtils.isEmpty(aggregationStr),
                "Metric " + metricIndex + "(" + metricName + "): " + "Aggregation missing for metric " + metricName);

        Aggregation aggregation;

        try {
            aggregation = Aggregation.valueOf(aggregationStr);
        } catch(Throwable t) {
            throw new IllegalArgumentException(
                    "Metric " + metricIndex + "(" + metricName + "): " +
                    "Invalid aggregation value " + aggregationStr +
                    " for metric, must be one of " + Aggregation.values());
        }

        return aggregation;
    }

    protected Type getDataTypeFromProperties(
            int metricIndex, Properties trackingMetricsProperties, String metricName, String suffix) {

        String dataTypeStr = trackingMetricsProperties.getProperty("metric." + metricIndex + ".datatype." + suffix);
        Assert.isTrue(!StringUtils.isEmpty(dataTypeStr), "Metric " + metricIndex + "(" + metricName + "): " +
                suffix + " data type missing for metric " + metricName);
        Assert.isTrue(
                dataTypeStr.equalsIgnoreCase(DATA_TYPE_LONG) ||
                        dataTypeStr.equalsIgnoreCase(DATA_TYPE_DOUBLE) ||
                        dataTypeStr.equalsIgnoreCase(DATA_TYPE_STRING),
                "Metric " + metricIndex + "(" + metricName + "): " +
                        "Invalid value for data type " + dataTypeStr + ", must be " +
                        DATA_TYPE_LONG + ", " + DATA_TYPE_DOUBLE + " or " + DATA_TYPE_STRING);

        Type dataType;

        if(dataTypeStr.equalsIgnoreCase(DATA_TYPE_LONG)) {
            dataType = Long.class;
        } else if(dataTypeStr.equalsIgnoreCase(DATA_TYPE_DOUBLE)) {
            dataType = Double.class;
        } else {
            dataType = String.class;
        }

        return dataType;
    }

}
