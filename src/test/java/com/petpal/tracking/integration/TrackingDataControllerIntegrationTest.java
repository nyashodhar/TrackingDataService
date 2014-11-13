package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.util.BucketCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;


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
public class TrackingDataControllerIntegrationTest extends AbstractTimeSeriesIntegrationTest {

    private TimeZone timeZonePDT;

    @Before
    public void setUp() {
        System.out.println("*** TrackingDataControllerIntegrationTest.setup(): port = " + port);
        timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");
    }

    //@Test
    public void test_timeZonePrintingStuff() {

        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JUNE, 2, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePDT);
        BucketCalculator.printUTCForCalendar(2014, Calendar.AUGUST, 1, 0, 0, 0, timeZonePDT);
    }

    //@Test
    public void test_get_metrics_boundary_test_empty_bucket_excluded() {

        String trackedEntityId = createTrackedEntityId();
        String trackingDeviceId = createTrackingDeviceId();

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PDT - 1401346800488
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT).getTimeInMillis();
        dataPoints.put(timeStamp1, 3L);

        // Data point 2: July 2nd, 2014, PDT - 1404284400598
        long timeStamp2 = BucketCalculator.getCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePDT).getTimeInMillis();
        dataPoints.put(timeStamp2, 2L);

        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);

        BucketCalculator.addDataPointForMetrics(testTrackingData, allMetrics, dataPoints);

        System.out.println("********* test_get_metrics_boundary_test_empty_bucket_excluded(): testTrackingData = " + testTrackingData);

        ResponseEntity<String> postResponse = postMetrics(testTrackingData);

        Map<TestTrackingMetric, Map<Long, Long>> getResponse = getMetrics(
                trackingDeviceId,
                BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT).getTimeInMillis(),
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

        long bucketKey = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePDT).getTimeInMillis();
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

        bucketKey = BucketCalculator.getCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePDT).getTimeInMillis();
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

        // Data point 1: May 29th, 2014, PDT
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePDT).getTimeInMillis();
        dataPoints.put(timeStamp1, 4L);

        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);

        BucketCalculator.addDataPointForMetrics(testTrackingData, allMetrics, dataPoints);

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


    @Test
    public void test_createTrackingData() {

        //String trackedEntityId = createTrackedEntityId();
        //String trackingDeviceId = createTrackingDeviceId();

        String trackedEntityId = "myentity3";
        String trackingDeviceId = "mydevice3";

        TestTrackingData testTrackingData = BucketCalculator.generateRandomTrackingData(
                trackedEntityId,
                trackingDeviceId,
                BucketCalculator.getCalendar(2014, Calendar.FEBRUARY, 2, 0, 0, 0, timeZonePDT),
                BucketCalculator.getCalendar(2014, Calendar.MARCH, 3, 0, 0, 0, timeZonePDT),
                60,
                120,
                50,
                50);

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

}
