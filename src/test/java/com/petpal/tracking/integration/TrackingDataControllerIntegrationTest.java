package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.service.TrackingMetric;
import com.petpal.tracking.util.BucketCalculator;
import com.petpal.tracking.util.JSONUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.ParameterizedTypeReference;
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

    private static final Long KAIROS_WRITE_DELAY = 1000L;

    @Value("${local.server.port}")
    int port;

    @Before
    public void setUp() {
        System.out.println("*** TrackingDataControllerIntegrationTest.setup(): port = " + port);
    }

    //@Test
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
    public void test_get_metrics_boundary_test_empty_bucket_excluded() {

        String trackedEntityId = createTrackedEntityId();
        String trackingDeviceId = createTrackingDeviceId();

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        TimeZone timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");

        // Data point 1: May 29th, 2014, PDT - 1401346800488
        long timeStamp1 = BucketCalculator.getUTCMillis(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT);
        dataPoints.put(timeStamp1, 3L);

        // Data point 2: July 2nd, 2014, PDT - 1404284400598
        long timeStamp2 = BucketCalculator.getUTCMillis(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePDT);
        dataPoints.put(timeStamp2, 2L);

        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);

        addDataPointForMetrics(testTrackingData, allMetrics, dataPoints);

        System.out.println("********* test_get_metrics_boundary_test_empty_bucket_excluded(): testTrackingData = " + testTrackingData);

        ResponseEntity<String> postResponse = postMetrics(testTrackingData);

        // Sleep a little to ensure read after write consistency

        Map<TestTrackingMetric, Map<Long, Long>> getResponse = getMetrics(
                trackingDeviceId,
                BucketCalculator.getUTCMillis(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT),
                null,
                TimeUnit.MONTHS,
                1,
                null,
                null);

        System.out.println("********* test_get_metrics_boundary_test_empty_bucket_excluded(): getResponse = " + getResponse);

        // There should be 4 metrics in the response, since we didn't itemize metrics

        Assert.assertEquals(4, getResponse.size());

        //
        // Bucket one should start at May 1, 2014 PDT
        // Bucket one should have a value of 3 for each metric
        //

        long bucketKey = BucketCalculator.getUTCMillis(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT);
        for(TestTrackingMetric metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 3L, getResponse);
        }

        //
        // We didn't request verbose response, so the empty June bucket will not be
        // present in the response
        //

        //
        // Bucket two should start at May 1, 2014 PDT
        // Bucket two should have a value of 2 for each metric
        //

        bucketKey = BucketCalculator.getUTCMillis(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePDT);
        for(TestTrackingMetric metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 2L, getResponse);
        }
    }


    //@Test
    public void metricTest() {

        String trackedEntityId = "tralalala";
        String trackingDeviceId = "blablabla";

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        TimeZone timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");

        // Data point 1: May 29th, 2014, PDT
        long timeStamp1 = BucketCalculator.getUTCMillis(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT);
        dataPoints.put(timeStamp1, 4L);

        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);

        addDataPointForMetrics(testTrackingData, allMetrics, dataPoints);

        ResponseEntity<String> responseData = postMetrics(testTrackingData);

        //
        // EXAMPLE ON HOW TO QUERY FOR THIS:
        //
        //   curl -v -X POST http://localhost:8080/api/v1/datapoints/query -H "Content-Type: application/json" -d '{ "start_absolute": 1357023600000, "metrics": [ { "tags": { "TRACKEDENTITY": ["tralalala"] }, "name": "WALKINGSTEPS", "aggregators": [ {"name": "sum", "sampling": {"value": 1,"unit": "days"} } ] } ] }'
        //
        // EXAMPLE RESULT:
        //
        //   {"queries":[{"sample_size":1,"results":[{"name":"WALKINGSTEPS","group_by":[{"name":"type","type":"number"}],"tags":{"TRACKEDENTITY":["tralalala"],"TRACKINGDEVICE":["blablabla"]},"values":[[1401346800000,3]]}]}]}
        //
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

    private void verifyValueForMetric(TestTrackingMetric testTrackingMetric, Long bucketKey, Long bucketValue, Map<TestTrackingMetric, Map<Long, Long>> metricResponse) {
        Map<Long, Long> bucketsForMetric = metricResponse.get(testTrackingMetric);
        Assert.assertNotNull(bucketsForMetric);
        Assert.assertEquals(bucketValue, bucketsForMetric.get(bucketKey));
    }


    private Map<TestTrackingMetric, Map<Long, Long>> getMetrics(
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
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            System.out.println("*** postMetrics(): response = " + response);

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
    }

    private static String createTrackingDeviceId() {
        return UUID.randomUUID().toString();
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
