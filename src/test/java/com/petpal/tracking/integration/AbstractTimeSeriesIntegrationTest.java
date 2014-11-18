package com.petpal.tracking.integration;

import com.petpal.tracking.util.JSONUtil;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Created by per on 11/13/14.
 */
public abstract class AbstractTimeSeriesIntegrationTest {

    private Logger logger = Logger.getLogger(this.getClass());

    protected static final Long KAIROS_WRITE_DELAY = 1000L;

    @Value("${local.server.port}")
    protected int port;

    protected ResponseEntity<String> postMetricsForDevice(String deviceId, TestTrackingData testTrackingData) {

        String json = JSONUtil.convertToString(testTrackingData);
        //System.out.println("*** Tracking data: " + testTrackingData);
        //System.out.println("*** Tracking data json: " + json);

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(json, headers);
        String url = "http://localhost:" + port + "/tracking/device/" + deviceId;

        try {
            logger.info("postMetrics(): doing post to " + url);
            long start = System.currentTimeMillis();
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            long end = System.currentTimeMillis();
            logger.info("postMetrics(): response received in " + (end - start) + " ms.");

            try {
                Thread.sleep(KAIROS_WRITE_DELAY);
            } catch(InterruptedException e) {
                logger.error("postMetrics(): Unexpected interrupted exception", e);
            }

            return response;
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                logger.error("postMetrics(): Unexpected server error: " + ((HttpServerErrorException) e).getStatusCode(), e);
            } else if(e instanceof HttpClientErrorException) {
                logger.error("postMetrics(): Unexpected client error: " + ((HttpClientErrorException) e).getStatusCode(), e);
            } else {
                logger.error("postMetrics(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode(), e);
            }
            throw e;
        }
    }


    protected Map<TestTrackingMetric, Map<Long, Long>> getAggregatedMetricsForDevice(
            String trackingDeviceId,
            Integer startYear,
            Integer startMonth,
            Integer startWeek,
            Integer startDay,
            Integer startHour,
            TimeUnit resultBucketSize,
            Integer bucketsToFetch,
            List<TestTrackingMetric> trackingMetrics,
            Boolean verboseResponse,
            TimeZone aggregationTimeZone) {

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(headers);

        String url = "http://localhost:" + port + "/metrics/device/{deviceId}?startYear={startYear}&resultBucketSize={resultBucketSize}";

        // Conditionally add optional query/url parameters

        Map<String, Object> urlArgs = new HashMap<String, Object>();
        urlArgs.put("deviceId", trackingDeviceId);
        urlArgs.put("startYear", startYear);
        urlArgs.put("resultBucketSize", resultBucketSize.toString());

        if(startMonth != null) {
            url = url + "&startMonth={startMonth}";
            urlArgs.put("startMonth", startMonth);
        }

        if(startWeek != null) {
            url = url + "&startWeek={startWeek}";
            urlArgs.put("startWeek", startWeek);
        }

        if(startDay != null) {
            url = url + "&startDay={startDay}";
            urlArgs.put("startDay", startDay);
        }

        if(startHour != null) {
            url = url + "&startHour={startHour}";
            urlArgs.put("startHour", startHour);
        }

        if(bucketsToFetch != null) {
            url = url + "&bucketsToFetch={bucketsToFetch}";
            urlArgs.put("bucketsToFetch", bucketsToFetch);
        }

        if(trackingMetrics != null) {
            url = url + "&trackingMetrics={trackingMetrics}";
            urlArgs.put("trackingMetrics", trackingMetricsToCommaSeparated(trackingMetrics));
        }

        if(verboseResponse != null) {
            url = url + "&verboseResponse={verboseResponse}";
            urlArgs.put("verboseResponse", verboseResponse);
        }

        if(aggregationTimeZone != null) {
            url = url + "&aggregationTimeZone={aggregationTimeZone}";
            urlArgs.put("aggregationTimeZone", aggregationTimeZone.getID());
        }

        //
        // EXAMPLE:
        //
        //    curl -v -X GET "http://localhost:9000/metrics/absolute/device/263e6c54-69c9-45f5-853c-b5f4420ceb5i?startYear=2014&resultBucketSize=YEARS&trackingMetrics=walkingsteps,runningsteps&verboseResponse=true" -H "Accept: application/json" -H "Content-Type: application/json"
        //

        //http://localhost:63549/metrics/device/a555bb6b-62ab-4e70-bf4a-06431fa8b5ec?startYear=2014&resultBucketSize=MONTHS&startMonth=4&startDay=&verboseResponse=4

        try {
            logger.info("Doing GET for metrics " + url);
            long start = System.currentTimeMillis();

            ResponseEntity<Map<TestTrackingMetric, Map<Long, Long>>> response =
                    restTemplate.exchange(url, HttpMethod.GET, entity,
                            new ParameterizedTypeReference<Map<TestTrackingMetric, Map<Long, Long>>>() {},
                            urlArgs);

            long end = System.currentTimeMillis();

            logger.info("Aggregated metric response received in " + (end - start) + "ms. Code: " + response.getStatusCode() + ", body: " + response.getBody());
            return response.getBody();
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                logger.error("getAggregatedMetricsForDevice(): Unexpected server error: " + ((HttpServerErrorException) e).getStatusCode(), e);
            } else if(e instanceof HttpClientErrorException) {
                logger.error("getAggregatedMetricsForDevice(): Unexpected client error: " + ((HttpClientErrorException) e).getStatusCode(), e);
            } else {
                logger.error("getAggregatedMetricsForDevice(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode(), e);
            }
            throw e;
        }
    }

    protected String trackingMetricsToCommaSeparated(List<TestTrackingMetric> trackingMetrics) {

        if(trackingMetrics == null) {
            return null;
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(TestTrackingMetric testTrackingMetric : trackingMetrics) {
            if(stringBuilder.length() > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append(testTrackingMetric.toString());
        }
        return stringBuilder.toString();
    }

    protected static String createTrackedEntityId() {
        return UUID.randomUUID().toString();
    }

    protected static String createTrackingDeviceId() {
        return UUID.randomUUID().toString();
    }

}
