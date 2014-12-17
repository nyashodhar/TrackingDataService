package com.petpal.tracking.web.validators;

import com.petpal.tracking.service.TrackingMetricConfig;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingDataUpload;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/2/14.
 */
@Component("trackingDataUploadValidator")
public class TrackingDataUploadValidator implements Validator {

    private static final Logger logger = Logger.getLogger(TrackingDataUploadValidator.class);

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    @Override
    public boolean supports(Class<?> paramClass) {
        return TrackingDataUpload.class.equals(paramClass);
    }

    @Override
    public void validate(Object obj, Errors errors) {

        if(obj == null) {
            logger.error("Null arg!");
            errors.reject("no.tracking.data.in.body");
            return;
        }

        TrackingDataUpload trackingData = (TrackingDataUpload) obj;

        // Perform some validation
        validateAndGetTypeInformation(trackingData, errors);
    }

    private void validateAndGetTypeInformation(TrackingDataUpload trackingData, Errors errors) {

        Map<String, TreeMap> metricsData = trackingData.getMetricsData();

        if(CollectionUtils.isEmpty(metricsData)) {
            logger.error("No metrics provided");
            errors.reject("no.tracking.data.in.body");
            return;
        }

        //
        // Validate that all metric names are valid, and that that for each name
        // the data has the correct datatype
        //

        for(String metricName : metricsData.keySet()) {

            TrackingMetricConfig trackingMetricConfig = null;

            try {
                trackingMetricConfig = trackingMetricsConfig.getTrackingMetric(metricName);
            } catch(IllegalArgumentException e) {
                logger.error("Unable to find any metric configuration for metric " + metricName);
                errors.reject("no.metric.config.found.for.metric.name");
                continue;
            }

            TreeMap dataPointsForMetric = metricsData.get(metricName);
            if(!dataPointsForMetric.isEmpty()) {
                //
                // Assume due to original deserialization that all objects in
                // in the map must be of same type, so we'll just look at the first one.
                //
                Object firstKey = dataPointsForMetric.keySet().iterator().next();

                // The key has to be a long (timestamp)

                if(!(firstKey instanceof Long)) {
                    logger.error("Key " + firstKey + " in metrics map for metric " + metricName + " is not a Long");
                    errors.reject("key.is.not.a.long.number");
                }

                // The value has to have the value specified in the metric config

                Object firstValue = dataPointsForMetric.get(firstKey);
                if(trackingMetricConfig.getRawDataType() == Long.class) {
                    if(!(firstValue instanceof Long)) {
                        logger.error("Value " + firstValue + " in metrics map for metric " + metricName + " is not a Long");
                        errors.reject("value.is.not.a.long");
                    }
                } else if(trackingMetricConfig.getRawDataType() == Double.class) {
                    if(!(firstValue instanceof Double)) {
                        logger.error("Value " + firstValue + " in metrics map for metric " + metricName + " is not a Double");
                        errors.reject("value.is.not.a.double");
                    }
                } else if(trackingMetricConfig.getRawDataType() == String.class) {
                    if(!(firstValue instanceof String)) {
                        logger.error("Value " + firstValue + " in metrics map for metric " + metricName + " is not a String");
                        errors.reject("value.is.not.a.string");
                    }
                }
            }
        }
    }

    public void setTrackingMetricsConfig(TrackingMetricsConfig trackingMetricsConfig) {
        this.trackingMetricsConfig = trackingMetricsConfig;
    }
}
