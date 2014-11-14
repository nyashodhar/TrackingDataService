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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by per on 11/13/14.
 */
public abstract class AbstractTimeSeriesIntegrationTest {

    private Logger logger = Logger.getLogger(this.getClass());

    protected static final Long KAIROS_WRITE_DELAY = 1000L;

    @Value("${local.server.port}")
    protected int port;

    protected ResponseEntity<String> postMetrics(TestTrackingData testTrackingData) {

        String json = JSONUtil.convertToString(testTrackingData);
        //System.out.println("*** Tracking data: " + testTrackingData);
        //System.out.println("*** Tracking data json: " + json);

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(json, headers);
        String url = "http://localhost:" + port + "/tracking";

        try {
            logger.info("postMetrics(): doing post to " + url);
            long start = System.currentTimeMillis();
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            long end = System.currentTimeMillis();
            logger.info("postMetrics(): response received in " + (end - start) + " ms.");

            try {
                Thread.sleep(KAIROS_WRITE_DELAY);
            } catch(InterruptedException e) {
                // Swallow
            }

            return response;
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                System.out.println("** postMetrics(): Unexpected server error: " + ((HttpServerErrorException) e).getStatusCode());
            } else if(e instanceof HttpClientErrorException) {
                System.out.println("** postMetrics(): Unexpected client error: " + ((HttpClientErrorException) e).getStatusCode());
            } else {
                System.out.println("** postMetrics(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode());
            }
            throw e;
        }
    }

    protected Map<TestTrackingMetric, Map<Long, Long>> getMetrics(
            String trackingDeviceId,
            long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            List<TestTrackingMetric> trackingMetrics,
            Boolean verboseResponse) {

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(headers);

        String url = "http://localhost:" + port + "/metrics/absolute/device/{deviceId}?utcBegin={utcBegin}&resultBucketSize={resultBucketSize}&resultBucketMultiplier={resultBucketMultiplier}";

        // Conditionally add optional query/url parameters

        if(utcEnd != null) {
            url = url + "&utcEnd={utcEnd}";
        }

        if(trackingMetrics != null) {
            url = url + "&trackingMetrics={trackingMetrics}";
        }

        if(verboseResponse != null) {
            url = url + "&verboseResponse={verboseResponse}";
        }

        //
        // EXAMPLE:
        //
        //    curl -v -X GET "http://localhost:9000/metrics/absolute/device/263e6c54-69c9-45f5-853c-b5f4420ceb5i?utcBegin=1398927600141&utcEnd=1406876400141&resultBucketSize=MONTHS&resultBucketMultiplier=1&trackingMetrics=walkingsteps,runningsteps&verboseResponse=true" -H "Accept: application/json" -H "Content-Type: application/json"
        //

        try {
            System.out.println("*** getMetrics(): doing GET");
            ResponseEntity<Map<TestTrackingMetric, Map<Long, Long>>> response =
                    restTemplate.exchange(url, HttpMethod.GET, entity,
                            new ParameterizedTypeReference<Map<TestTrackingMetric, Map<Long, Long>>>() {},
                            trackingDeviceId,
                            utcBegin,
                            resultBucketSize.toString(),
                            resultBucketMultiplier,
                            utcEnd,
                            trackingMetricsToCommaSeparated(trackingMetrics),
                            verboseResponse);
            System.out.println("** getMetrics(): response = " + response.getBody());
            return response.getBody();
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                System.out.println("** getMetrics(): Unexpected server error: " + ((HttpServerErrorException) e).getStatusCode());
            } else if(e instanceof HttpClientErrorException) {
                System.out.println("** getMetrics(): Unexpected client error: " + ((HttpClientErrorException) e).getStatusCode());
            } else {
                System.out.println("** getMetrics(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode());
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

    protected List<TestTrackingMetric> getAllTrackingMetrics() {
        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);
        return allMetrics;
    }

    protected static String createTrackedEntityId() {
        return UUID.randomUUID().toString();
    }

    protected static String createTrackingDeviceId() {
        return UUID.randomUUID().toString();
    }


}
