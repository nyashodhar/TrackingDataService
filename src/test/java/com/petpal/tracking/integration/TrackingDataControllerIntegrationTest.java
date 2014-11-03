package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.util.BucketCalculator;
import com.petpal.tracking.util.JSONUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;


/**
 * Created by per on 10/28/14.
 *
 * Useful blogpost on how to create an integration test:
 *
 *   http://www.jayway.com/2014/07/04/integration-testing-a-spring-boot-application/
 */

@RunWith(SpringJUnit4ClassRunner.class) //
@SpringApplicationConfiguration(classes = TrackingDataServiceConfiguration.class)
@WebAppConfiguration
@IntegrationTest({"server.port:0","management.port:0"})   // Will start the server on a random port
public class TrackingDataControllerIntegrationTest {

    @Value("${local.server.port}")
    int port;

    @Before
    public void setUp() {
        System.out.println("*** TrackingDataControllerIntegrationTest.setup(): port = " + port);
    }

    @Test
    public void test_timeZoneStuff() {

        TimeZone timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");
        //TimeZone timeZoneUTC = TimeZone.getTimeZone("UTC");

        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JUNE, 2, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.AUGUST, 1, 0, 0, 0, timeZonePDT);
    }

    @Test
    public void test_boundaryTest() {

        String trackedEntityId = "c45c4cd8-06fd-41be-aa0c-76a5418d3027";
        String trackingDeviceId = "263e6c54-69c9-45f5-853c-b5f4420ceb5k";

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PDT - 1401346800488
        dataPoints.put(1401346800488L, 3L);

        // Data point 2: July 2nd, 2014, PDT - 1404284400598
        dataPoints.put(1404284400598L, 2L);

        List<TestTrackingMetric> metrics = new ArrayList<TestTrackingMetric>();
        metrics.add(TestTrackingMetric.WALKINGSTEPS);
        metrics.add(TestTrackingMetric.RUNNINGSTEPS);
        metrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        metrics.add(TestTrackingMetric.RESTINGSECONDS);

        addDataPointForMetrics(testTrackingData, metrics, dataPoints);

        ResponseEntity<String> postResponse = postMetrics(testTrackingData);

        ResponseEntity<String> getResponse = getMetrics(
                trackingDeviceId,
                1398927600141L,
                null,
                TimeUnit.MONTHS,
                1,
                null,
                null);
    }


    //@Test
    public void test_createTrackingData() {

        String trackedEntityId = createTrackedEntityId();
        String trackingDeviceId = createTrackingDeviceId();

        // 5 minutes..
        //TrackingData trackingData = generateTrackingData(5, 80, 200, 60, 60);

        // 1 year..
        TestTrackingData testTrackingData = generateRandomTrackingData(trackedEntityId, trackingDeviceId, 525600, 80, 200, 60, 60);
        ResponseEntity<String> responseData = postMetrics(testTrackingData);
    }


    //
    // Utility methods
    //


    private ResponseEntity<String> getMetrics(
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
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, entity, String.class,
                    trackingDeviceId,
                    utcBegin,
                    resultBucketSize.toString(),
                    resultBucketMultiplier,
                    utcEnd,
                    trackingMetricsToCommaSeparated(trackingMetrics),
                    verboseResponse);
            System.out.println("** getMetrics(): response = " + response.getBody());
            return response;
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

    private ResponseEntity<String> postMetrics(TestTrackingData testTrackingData) {

        String json = JSONUtil.convertToString(testTrackingData);
        System.out.println("*** Tracking data to string: " + testTrackingData);
        System.out.println("*** Tracking data json: " + json);

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(json, headers);
        String url = "http://localhost:" + port + "/tracking";

        try {
            System.out.println("*** postMetrics(): doing post");
            //ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/tracking", json, String.class);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            System.out.println("*** postMetrics(): response = " + response);
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


    private void addDataPointForMetrics(TestTrackingData testTrackingData, List<TestTrackingMetric> metrics, Map<Long, Long> dataPoints) {

        for(TestTrackingMetric metric : metrics) {

            if(metric == TestTrackingMetric.WALKINGSTEPS) {
                testTrackingData.setWalkingData(dataPoints);
            } else if(metric == TestTrackingMetric.RUNNINGSTEPS) {
                testTrackingData.setRunningData(dataPoints);
            } else if(metric == TestTrackingMetric.SLEEPINGSECONDS) {
                testTrackingData.setSleepingData(dataPoints);
            } else if(metric == TestTrackingMetric.RESTINGSECONDS) {
                testTrackingData.setRestingData(dataPoints);
            }
        }
    }

    private TestTrackingData generateRandomTrackingData(
            String trackedEntityId,
            String trackingDeviceId,
            int minutesIntoPast,
            int maxWalkingStepsPerMinute, int maxRunningStepsPerMinute,
            int maxSleepSecondsPerMinute, int maxRestSeconds) {

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);
        addTimeData(testTrackingData, minutesIntoPast,
            maxWalkingStepsPerMinute, maxRunningStepsPerMinute,
            maxSleepSecondsPerMinute, maxRestSeconds);

        return testTrackingData;
    }

    private void addTimeData(TestTrackingData testTrackingData,
                             int minutesIntoPast, int maxWalkingStepsPerBucket, int maxRunningStepsPerMinute,
                             int maxSleepSecondsPerMinute, int maxRestSecondsPerMinute) {

        Map<Long, Long> walkingData = generateTimeData(minutesIntoPast, maxWalkingStepsPerBucket);
        Map<Long, Long> runningData = generateTimeData(minutesIntoPast, maxRunningStepsPerMinute);
        Map<Long, Long> sleepingData = generateTimeData(minutesIntoPast, maxSleepSecondsPerMinute);
        Map<Long, Long> restingData = generateTimeData(minutesIntoPast, maxRestSecondsPerMinute);

        testTrackingData.setWalkingData(walkingData);
        testTrackingData.setRunningData(runningData);
        testTrackingData.setSleepingData(sleepingData);
        testTrackingData.setRestingData(restingData);
    }

    private Map<Long, Long> generateTimeData(int minutesIntoPast, int maxRandomValueForMetric) {

        Map<Long, Long> timeData = new HashMap<Long, Long>();

        for(int i=0; i<minutesIntoPast; i++) {
            long bucketTimeStamp = (System.currentTimeMillis() - 60L*1000L*i);
            long metricValue = randInt(0, maxRandomValueForMetric);
            timeData.put(bucketTimeStamp, metricValue);
        }

        return timeData;
    }

    private static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

    private static String createTrackedEntityId() {
        return UUID.randomUUID().toString();
        //return TRACKED_ENTITY_ID;
    }

    private static String createTrackingDeviceId() {
        return UUID.randomUUID().toString();
        //return TRACKING_DEVICE_ID;
    }

    private String trackingMetricsToCommaSeparated(List<TestTrackingMetric> trackingMetrics) {

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
}
