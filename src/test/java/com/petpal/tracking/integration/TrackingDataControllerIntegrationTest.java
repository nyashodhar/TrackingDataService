package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.util.BucketCalculator;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

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

    private Logger logger = Logger.getLogger(this.getClass());

    private TimeZone timeZonePST;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;


    @Before
    public void setUp() {
        logger.info("TrackingDataControllerIntegrationTest.setup(): port = " + port);
        timeZonePST = TimeZone.getTimeZone("PST");
    }

    //@Test
    public void test_timeZonePrintingStuff() {

        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JUNE, 2, 0, 0, 0, timeZonePST);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePST);
        BucketCalculator.printUTCForCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST);
        BucketCalculator.printUTCForCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePST);
        BucketCalculator.printUTCForCalendar(2014, Calendar.AUGUST, 1, 0, 0, 0, timeZonePST);
    }

    @Test
    public void test_get_metrics_for_device_boundary_test_empty_bucket_excluded() {

        String trackingDeviceId = createTrackingDeviceId();

        TestTrackingData testTrackingData = new TestTrackingData();

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PST - 1401346800488
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp1, 3L);

        // Data point 2: July 2nd, 2014, PST - 1404284400598
        long timeStamp2 = BucketCalculator.getCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp2, 2L);

        List<TestTrackingMetric> allMetrics = TestTrackingMetric.getAllTrackingMetrics();

        BucketCalculator.addDataPointForAllMetrics(testTrackingData, dataPoints);

        ResponseEntity<String> postResponse = postMetricsForDevice(trackingDeviceId, testTrackingData, timeZonePST);

        blockUntilAsyncThreadIdleInServer();

        Map<TestTrackingMetric, Map<Long, Long>> getResponse = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TimeUnit.MONTHS,
                2014,
                Calendar.MAY,
                null,
                null,
                null,
                null,
                null,
                false,
                timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics

        Assert.assertEquals(4, getResponse.size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 3 for each metric
        //

        long bucketKey = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(TestTrackingMetric metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 3L, getResponse);
        }

        //
        // We didn't request verbose response, so the empty June bucket will not be
        // present in the response
        //

        //
        // Bucket two should start at July 1, 2014 PST
        // Bucket two should have a value of 2 for each metric
        //

        bucketKey = BucketCalculator.getCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(TestTrackingMetric metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 2L, getResponse);
        }
    }


    //@Test
    public void metricTest() {

        String trackingDeviceId = "blablabla";

        TestTrackingData testTrackingData = new TestTrackingData();

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PST
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp1, 4L);

        BucketCalculator.addDataPointForAllMetrics(testTrackingData, dataPoints);

        ResponseEntity<String> responseData = postMetricsForDevice(trackingDeviceId, testTrackingData, timeZonePST);

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
    public void test_metrics_for_device_year_aggregation() {

        String trackingDeviceId = createTrackingDeviceId();

        //
        // Do the first post followed by a GET to validate aggregated data
        //

        TestTrackingData testTrackingData1 = BucketCalculator.generateRandomTrackingData(
                BucketCalculator.getCalendar(2012, Calendar.MAY, 7, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2012, Calendar.MAY, 20, 0, 0, 0, timeZonePST));

        TestTrackingData testTrackingData2 = BucketCalculator.generateRandomTrackingData(
                BucketCalculator.getCalendar(2014, Calendar.FEBRUARY, 2, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2014, Calendar.MARCH, 3, 0, 0, 0, timeZonePST));

        TestTrackingData combinedTestTrackingData1 = BucketCalculator.combineTrackingData(testTrackingData1, testTrackingData2);

        ResponseEntity<String> responseData1 = postMetricsForDevice(trackingDeviceId, combinedTestTrackingData1, timeZonePST);
        blockUntilAsyncThreadIdleInServer();

        Map<TestTrackingMetric, Map<Long, Long>> getResponse1 = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TimeUnit.YEARS,
                2012,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                timeZonePST);

        Assert.assertEquals("Unexpected number of metric types in get response", 4, getResponse1.size());

        for(TestTrackingMetric testTrackingMetric : TestTrackingMetric.getAllTrackingMetrics()) {

            Assert.assertEquals("Unexpected number of year buckets types in get response for metric " + testTrackingMetric, 2, getResponse1.get(testTrackingMetric).size());

            long firstYearExpectedValue = sumValuesFromTrackingData(testTrackingData1, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2012, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(), firstYearExpectedValue, getResponse1);

            long secondYearExpectedValue = sumValuesFromTrackingData(testTrackingData2, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2014, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(), secondYearExpectedValue, getResponse1);
        }

        //
        // Do a 2nd post and a GET to validate updated aggregated data
        //

        TestTrackingData testTrackingData3 = BucketCalculator.generateRandomTrackingData(
                BucketCalculator.getCalendar(2012, Calendar.MAY, 22, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2012, Calendar.MAY, 27, 0, 0, 0, timeZonePST));

        TestTrackingData testTrackingData4 = BucketCalculator.generateRandomTrackingData(
                BucketCalculator.getCalendar(2014, Calendar.AUGUST, 9, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2014, Calendar.AUGUST, 23, 0, 0, 0, timeZonePST));

        TestTrackingData combinedTestTrackingData2 = BucketCalculator.combineTrackingData(testTrackingData3, testTrackingData4);
        ResponseEntity<String> responseData2 = postMetricsForDevice(trackingDeviceId, combinedTestTrackingData2, timeZonePST);
        blockUntilAsyncThreadIdleInServer();


        Map<TestTrackingMetric, Map<Long, Long>> getResponse2 = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TimeUnit.YEARS,
                2012,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                timeZonePST);

        Assert.assertEquals("Unexpected number of metric types in 2nd get response", 4, getResponse2.size());

        for(TestTrackingMetric testTrackingMetric : TestTrackingMetric.getAllTrackingMetrics()) {

            Assert.assertEquals("Unexpected number of year buckets types in 2nd get response for metric " + testTrackingMetric, 2, getResponse2.get(testTrackingMetric).size());

            long firstYearExpectedValue = sumValuesFromTrackingData(testTrackingData1, testTrackingMetric);
            firstYearExpectedValue = firstYearExpectedValue + sumValuesFromTrackingData(testTrackingData3, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2012, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(), firstYearExpectedValue, getResponse2);

            long secondYearExpectedValue = sumValuesFromTrackingData(testTrackingData2, testTrackingMetric);
            secondYearExpectedValue = secondYearExpectedValue + sumValuesFromTrackingData(testTrackingData4, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2014, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(), secondYearExpectedValue, getResponse2);
        }
    }

    //
    // Utility methods
    //

    private long sumValuesFromTrackingData(TestTrackingData testTrackingData, TestTrackingMetric testTrackingMetric) {

        Map<Long, Long> mapToSumValuesFrom = testTrackingData.getDataForMetric(testTrackingMetric);

        long sum = 0;

        for(long value : mapToSumValuesFrom.values()) {
            sum = sum + value;
        }

        return sum;
    }

    private void verifyValueForMetric(
            TestTrackingMetric testTrackingMetric,
            Long bucketKey,
            Long expectedBucketValue,
            Map<TestTrackingMetric,
            Map<Long, Long>> metricResponse) {

        Map<Long, Long> bucketsForMetric = metricResponse.get(testTrackingMetric);
        Assert.assertNotNull("No bucket found for metric " + testTrackingMetric, bucketsForMetric);
        Assert.assertEquals("Unexpected value found for metric " + testTrackingMetric, expectedBucketValue, bucketsForMetric.get(bucketKey));
    }

    private void blockUntilAsyncThreadIdleInServer() {
        // Block until the server has no active threads in the async thread pool executor.
        boolean threadsNotDone = true;
        while(threadsNotDone) {
            threadsNotDone = (threadPoolTaskExecutor.getActiveCount() > 0);
            if(threadsNotDone) {
                logger.debug("Threads not done: sleep 1 sec.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                logger.debug("All threads done!");
                try {
                    Thread.sleep(KAIROS_WRITE_DELAY);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
