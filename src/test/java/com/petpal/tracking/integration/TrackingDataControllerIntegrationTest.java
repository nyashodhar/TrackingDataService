package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.util.JSONUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import java.util.GregorianCalendar;
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

    private static final String TRACKED_ENTITY_ID = "c45c4cd8-06fd-41be-aa0c-76a5418d3021";
    private static final String TRACKING_DEVICE_ID = "263e6c54-69c9-45f5-853c-b5f4420ceb5e";

    @Value("${local.server.port}")   // 6
    int port;

    @Before
    public void setUp() {

        System.out.println("*** HelloWorldControllerIntegrationTest.setup(): port = " + port);

    }

    @Test
    public void test_timeZoneStuff() {

        TimeZone timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");
        TimeZone timeZoneUTC = TimeZone.getTimeZone("UTC");

        /*
        Calendar calendar1 = new GregorianCalendar();
        calendar1.setTimeZone(timeZonePDT);
        calendar1.set(2014, Calendar.MAY, 1, 0, 0, 0);
        System.out.println("May 1st 2014 PDT, Midnight: Millis: " + calendar1.getTimeInMillis());
        System.out.println("May 1st 2014 PDT, Midnight: Date: " + calendar1.getTime());

        Calendar calendar2 = new GregorianCalendar();
        calendar2.setTimeZone(timeZonePDT);
        calendar2.set(2014, Calendar.JUNE, 1, 0, 0, 0);
        System.out.println("June 31st 2014 PDT, Midnight: UTC Millis: " + calendar2.getTimeInMillis());
        System.out.println("June 31st 2014 PDT, Midnight: Date: " + calendar2.getTime());

        calendar2.setTimeZone(timeZoneUTC);

        //
        // NOTE: Just changing the timezone doesn't make the UTC millis recompute to happen based
        // on the specified time. The old computation follows the previously spec computation with
        // outcome derived from whatever timezone was set at that time. So we need to do another
        // time set to get the UTC millis to change...
        //

        calendar2.set(2014, Calendar.OCTOBER, 31, 0, 0, 0);
        System.out.println("Oct 31st 2014 UTC, Midnight: UTC Millis: " + calendar2.getTimeInMillis());
        System.out.println("Oct 31st 2014 UTC, Midnight: Date: " + calendar2.getTime());
        */

        printUTCForCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT);
        printUTCForCalendar(2014, Calendar.JUNE, 2, 0, 0, 0, timeZonePDT);
        printUTCForCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePDT);
        printUTCForCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT);
        printUTCForCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePDT);
        printUTCForCalendar(2014, Calendar.AUGUST, 1, 0, 0, 0, timeZonePDT);
    }

    private void printUTCForCalendar(int year, int month, int date, int hour, int minute, int second, TimeZone timeZone) {

        Calendar calendar = new GregorianCalendar();
        calendar.setTimeZone(timeZone);
        calendar.set(year, month, date, hour, minute, second);
        System.out.println("printUTCForCalendar: calendar.getTime() = " + calendar.getTime() + ", utc millis = " + calendar.getTimeInMillis());
    }

    @Test
    public void test_boundaryTest() {

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId("c45c4cd8-06fd-41be-aa0c-76a5418d3025");
        testTrackingData.setTrackingDeviceId("263e6c54-69c9-45f5-853c-b5f4420ceb5i");

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

        ResponseEntity<String> response = postMetrics(testTrackingData);
    }


    //@Test
    public void test_createTrackingData() {
        System.out.println("*** HelloWorldControllerIntegrationTest.test_hello(): port = " + port);

        String trackedEntityId = createTrackedEntityId();
        String trackingDeviceId = createTrackingDeviceId();

        // 5 minutes..
        //TrackingData trackingData = generateTrackingData(5, 80, 200, 60, 60);

        // 1 year..
        TestTrackingData testTrackingData = generateRandomTrackingData(trackedEntityId, trackingDeviceId, 525600, 80, 200, 60, 60);
        ResponseEntity<String> responseData = postMetrics(testTrackingData);
    }


    //
    //
    //


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
            System.out.println("*** HelloWorldControllerIntegrationTest.test_hello(): doing post");
            //ResponseEntity<String> response = restTemplate.postForEntity("http://localhost:" + port + "/tracking", json, String.class);
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            System.out.println("*** HelloWorldControllerIntegrationTest.test_hello(): response = " + response);
            return response;
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                System.out.println("** HelloWorldControllerIntegrationTest.test_hello(): Unexpected server error: " + ((HttpServerErrorException) e).getStatusCode());
            } else if(e instanceof HttpClientErrorException) {
                System.out.println("** HelloWorldControllerIntegrationTest.test_hello(): Unexpected client error: " + ((HttpClientErrorException) e).getStatusCode());
            } else {
                System.out.println("** HelloWorldControllerIntegrationTest.test_hello(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode());
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

}
