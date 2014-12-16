package com.petpal.tracking.integration;

import com.petpal.tracking.util.JSONUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;
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

    protected ResponseEntity<String> postMetricsForDevice(
            String deviceId, TestTrackingDataUpload testTrackingData, TimeZone aggregationTimeZone) {

        String json = JSONUtil.convertToString(testTrackingData);
        //System.out.println("*** Tracking data: " + testTrackingData);
        //System.out.println("*** Tracking data json: " + json);

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(json, headers);
        String url = "http://localhost:" + port + "/tracking/device/" + deviceId;

        if(aggregationTimeZone != null) {
            url = url + "?aggregationTimeZone=" + aggregationTimeZone.getID();
        }

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


    protected TestTrackingDataDownload getAggregatedMetricsForDevice(
            String trackingDeviceId,
            String aggregationLevel,
            Integer startYear,
            Integer startMonth,
            Integer startWeek,
            Integer startDay,
            Integer startHour,
            Integer bucketsToFetch,
            List<String> trackingMetrics,
            TimeZone aggregationTimeZone) {

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(headers);

        String url = "http://localhost:" + port + "/metrics/device/{deviceId}/aggregate/{aggregationLevel}?startYear={startYear}";

        // Conditionally add optional query/url parameters

        Map<String, Object> urlArgs = new HashMap<String, Object>();
        urlArgs.put("deviceId", trackingDeviceId);
        urlArgs.put("aggregationLevel", aggregationLevel);
        urlArgs.put("startYear", startYear);

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

        if(aggregationTimeZone != null) {
            url = url + "&aggregationTimeZone={aggregationTimeZone}";
            urlArgs.put("aggregationTimeZone", aggregationTimeZone.getID());
        }

        try {
            logger.info("Doing GET for metrics " + url);
            long start = System.currentTimeMillis();

            ResponseEntity<TestTrackingDataDownload> response =
                    restTemplate.exchange(url, HttpMethod.GET, entity,
                            new ParameterizedTypeReference<TestTrackingDataDownload>() {},
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

    protected String trackingMetricsToCommaSeparated(List<String> trackingMetrics) {

        if(trackingMetrics == null) {
            return null;
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(String testTrackingMetric : trackingMetrics) {
            if(stringBuilder.length() > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append(testTrackingMetric.toString());
        }
        return stringBuilder.toString();
    }


    protected void check400Response(RestClientException e) {

        Assert.assertTrue(e instanceof HttpClientErrorException);
        HttpClientErrorException err = (HttpClientErrorException) e;
        Assert.assertEquals(err.getStatusCode(), HttpStatus.BAD_REQUEST);

        Map<String, Serializable> mappedJson = JSONUtil.jsonToMap(err.getResponseBodyAsString());

        Assert.assertEquals(mappedJson.get("status").toString(), Integer.toString(HttpStatus.BAD_REQUEST.value()));
        //Assert.assertNotNull(mappedJson.get("error"));

        // Ensure the timestamp is very close to 'now'
        long timestampInResponse = Long.parseLong(mappedJson.get("timestamp").toString());
        Assert.assertTrue(System.currentTimeMillis()-3000L < timestampInResponse);
        Assert.assertTrue(System.currentTimeMillis()+3000L > timestampInResponse);
    }


    protected static String createTrackedEntityId() {
        return UUID.randomUUID().toString();
    }

    protected static String createTrackingDeviceId() {
        return UUID.randomUUID().toString();
    }

}
