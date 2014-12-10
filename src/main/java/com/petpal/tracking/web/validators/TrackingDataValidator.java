package com.petpal.tracking.web.validators;

import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingDataTest;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * Created by per on 12/2/14.
 */
@Component("trackingDataValidator")
public class TrackingDataValidator implements Validator {

    private static final Logger logger = Logger.getLogger(TrackingDataValidator.class);

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    @Override
    public boolean supports(Class<?> paramClass) {
        return TrackingDataTest.class.equals(paramClass);
    }

    @Override
    public void validate(Object obj, Errors errors) {

        if(obj == null) {
            logger.error("Null arg!");
            errors.reject("no.tracking.data.in.body");
            return;
        }

        TrackingDataTest trackingData = (TrackingDataTest) obj;

        if(CollectionUtils.isEmpty(trackingData.getLongMetrics()) &&
                CollectionUtils.isEmpty(trackingData.getDoubleMetrics()) &&
                CollectionUtils.isEmpty(trackingData.getStringMetrics())) {

            logger.error("No metrics collections of any type provided");
            errors.reject("no.tracking.data.in.body");
            return;
        }

        //
        // Validate that the type is correct as expected by the time series
        // configuration
        //

        validateMetricName(Long.class, trackingData, errors);
        validateMetricName(Double.class, trackingData, errors);
        validateMetricName(String.class, trackingData, errors);
    }

    protected void validateMetricName(Type type, TrackingDataTest trackingData, Errors errors) {

        Set<String> metricNames;

        if(type == Long.class) {
            metricNames = trackingData.getLongMetrics().keySet();
        } else if(type == Double.class) {
            metricNames = trackingData.getDoubleMetrics().keySet();
        } else {
            metricNames = trackingData.getStringMetrics().keySet();
        }

        for(String metricName : metricNames) {
            try {
                trackingMetricsConfig.getTrackingMetric(metricName);
            } catch(IllegalArgumentException e) {
                logger.error("Unable to find any metric configuration for " + type + " metric " + metricName);
                errors.reject("no.metric.config.found.for.metric.name");
            }
        }
    }

    public void setTrackingMetricsConfig(TrackingMetricsConfig trackingMetricsConfig) {
        this.trackingMetricsConfig = trackingMetricsConfig;
    }
}
