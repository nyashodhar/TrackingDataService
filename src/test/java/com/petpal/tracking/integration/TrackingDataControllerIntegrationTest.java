package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.util.BucketCalculator;
import com.petpal.tracking.util.TrackingMetricConfigUtil;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestClientException;

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

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TrackingDataServiceConfiguration.class)
@WebAppConfiguration
@IntegrationTest({"server.port:0","management.port:0"}) // Will start the server on a random port
public class TrackingDataControllerIntegrationTest extends AbstractTimeSeriesIntegrationTest {

    private Logger logger = Logger.getLogger(this.getClass());

    private TimeZone timeZonePST;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    @Before
    public void setUp() {
        logger.info("TrackingDataControllerIntegrationTest.setup(): port = " + port);
        timeZonePST = TimeZone.getTimeZone("PST");
    }

    @Test
    public void testGetAggregatedMetricForDevice_invalid_bucket_size_400() {
        try {
            getAggregatedMetricsForDevice("deviceid", "SECONDS", 2014, 1, null, null, null, 1, null, timeZonePST);
            Assert.fail("Request for metrics with invalid bucket size should have given 400 error");
        } catch(RestClientException e) {
            check400Response(e);
        }
    }

    @Test
    public void testGetAggregatedMetricForDevice_start_year_missing_400() {
        try {
            Integer startYear = null;
            getAggregatedMetricsForDevice("deviceid", TestAggregationLevel.YEARS.toString(), startYear, null, null, null, null, 1, null, timeZonePST);
            Assert.fail("Request for metrics with missing start year should have given 400 error");
        } catch(RestClientException e) {
            check400Response(e);
        }
    }


    @Test
    public void testGetAggregatedMetricForDevice_invalid_arg_combo_400() {
        try {
            getAggregatedMetricsForDevice("deviceid", TestAggregationLevel.YEARS.toString(), 2014, 3, null, null, null, 1, null, timeZonePST);
            Assert.fail("Request for metrics with for year, but month is also specified, should have given 400 error");
        } catch(RestClientException e) {
            check400Response(e);
        }
    }

    @Test
    public void testDataForSameBucketForDifferentDevices() {

        String trackingDeviceId1 = createTrackingDeviceId();
        String trackingDeviceId2 = createTrackingDeviceId();

        long timeStamp = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST).getTimeInMillis();

        TreeMap<Long, Long> dataPoints1 = new TreeMap<Long, Long>();
        dataPoints1.put(timeStamp, 3L);

        List<String> allLongMetrics = TrackingMetricConfigUtil.getAllLongTypeMetrics();

        TestTrackingDataUpload testTrackingData1 = new TestTrackingDataUpload();
        BucketCalculator.addDataPointForAllLongMetrics(testTrackingData1, dataPoints1);

        ResponseEntity<String> postResponse1 = postMetricsForDevice(trackingDeviceId1, testTrackingData1, timeZonePST);

        blockUntilAsyncThreadIdleInServer();

        TestTrackingDataDownload testTrackingDataDownload1 = getAggregatedMetricsForDevice(
                trackingDeviceId1, TestAggregationLevel.MONTHS.toString(), 2014, Calendar.MAY, null, null, null, null, null, timeZonePST);

        // There should be 4 long metrics in the response, since we didn't itemize metrics
        Assert.assertEquals(4, testTrackingDataDownload1.getLongMetrics().size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 3 for each metric
        //

        long bucketKey = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(String longMetric : testTrackingDataDownload1.getLongMetrics().keySet()) {
            verifyValueForMetric(longMetric, bucketKey, 3L, testTrackingDataDownload1.getLongMetrics());
        }

        // Insert data that will be aggregated into the same bucket for a different device...

        TreeMap<Long, Long> dataPoints2 = new TreeMap<Long, Long>();
        dataPoints2.put(timeStamp, 7L);

        TestTrackingDataUpload testTrackingData2 = new TestTrackingDataUpload();
        BucketCalculator.addDataPointForAllLongMetrics(testTrackingData2, dataPoints2);

        ResponseEntity<String> postResponse2 = postMetricsForDevice(trackingDeviceId2, testTrackingData2, timeZonePST);

        blockUntilAsyncThreadIdleInServer();

        TestTrackingDataDownload testTrackingDataDownload2 = getAggregatedMetricsForDevice(
                trackingDeviceId2, TestAggregationLevel.MONTHS.toString(), 2014, Calendar.MAY, null, null, null, null, null, timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics
        Assert.assertEquals(4, testTrackingDataDownload2.getLongMetrics().size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 7 for each metric
        //

        //long bucketKey = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(String longMetric : testTrackingDataDownload2.getLongMetrics().keySet()) {
            verifyValueForMetric(longMetric, bucketKey, 7L, testTrackingDataDownload2.getLongMetrics());
        }

        // Repeat the query for the data for device1 and check that result is still the same

        testTrackingDataDownload1 = getAggregatedMetricsForDevice(
                trackingDeviceId1, TestAggregationLevel.MONTHS.toString(), 2014, Calendar.MAY, null, null, null, null, null, timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics
        Assert.assertEquals(4, testTrackingDataDownload1.getLongMetrics().size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 3 for each metric
        //

        for(String longMetric : testTrackingDataDownload1.getLongMetrics().keySet()) {
            verifyValueForMetric(longMetric, bucketKey, 3L, testTrackingDataDownload1.getLongMetrics());
        }

        // Add a 2nd value for device1 in the same bucket

        timeStamp = BucketCalculator.getCalendar(2014, Calendar.MAY, 27, 0, 0, 0, timeZonePST).getTimeInMillis();

        TreeMap<Long, Long> dataPoints1_1 = new TreeMap<Long, Long>();
        dataPoints1_1.put(timeStamp, 11L);

        TestTrackingDataUpload testTrackingData1_1 = new TestTrackingDataUpload();
        BucketCalculator.addDataPointForAllLongMetrics(testTrackingData1_1, dataPoints1_1);

        postMetricsForDevice(trackingDeviceId1, testTrackingData1_1, timeZonePST);

        blockUntilAsyncThreadIdleInServer();

        testTrackingDataDownload1 = getAggregatedMetricsForDevice(
                trackingDeviceId1, TestAggregationLevel.MONTHS.toString(), 2014, Calendar.MAY, null, null, null, null, null, timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics
        Assert.assertEquals(4, testTrackingDataDownload1.getLongMetrics().size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 14 for each metric
        //

        for(String longMetric : testTrackingDataDownload1.getLongMetrics().keySet()) {
            verifyValueForMetric(longMetric, bucketKey, 14L, testTrackingDataDownload1.getLongMetrics());
        }

        // Repeat the query for the data for device2 and check that result is still the same

        testTrackingDataDownload2 = getAggregatedMetricsForDevice(
                trackingDeviceId2, TestAggregationLevel.MONTHS.toString(), 2014, Calendar.MAY, null, null, null, null, null, timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics
        Assert.assertEquals(4, testTrackingDataDownload2.getLongMetrics().size());

        //
        // Bucket one should start at May 1, 2014 PST
        // Bucket one should have a value of 7 for each metric
        //

        for(String longMetric : testTrackingDataDownload2.getLongMetrics().keySet()) {
            verifyValueForMetric(longMetric, bucketKey, 7L, testTrackingDataDownload2.getLongMetrics());
        }
    }


    @Test
    public void testGetAggregatedMetricForDevice_boundary_test_empty_bucket_excluded() {

        String trackingDeviceId = createTrackingDeviceId();

        TestTrackingDataUpload testTrackingDataUpload = new TestTrackingDataUpload();

        TreeMap<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PST - 1401346800488
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp1, 3L);

        // Data point 2: July 2nd, 2014, PST - 1404284400598
        long timeStamp2 = BucketCalculator.getCalendar(2014, Calendar.JULY, 2, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp2, 2L);

        // Data point 3: Aug 22nd, 2014
        long timeStamp3 = BucketCalculator.getCalendar(2014, Calendar.AUGUST, 22, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp3, 233L);


        List<String> allMetrics = TrackingMetricConfigUtil.getAllLongTypeMetrics();

        BucketCalculator.addDataPointForAllLongMetrics(testTrackingDataUpload, dataPoints);

        ResponseEntity<String> postResponse = postMetricsForDevice(trackingDeviceId, testTrackingDataUpload, timeZonePST);

        blockUntilAsyncThreadIdleInServer();

        TestTrackingDataDownload testTrackingDataDownload = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TestAggregationLevel.MONTHS.toString(),
                2014,
                Calendar.MAY,
                null,
                null,
                null,
                null,
                null,
                timeZonePST);

        // There should be 4 metrics in the response, since we didn't itemize metrics

        Assert.assertEquals(4, testTrackingDataDownload.getLongMetrics().size());

        long bucketKey = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(String metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 3L, testTrackingDataDownload.getLongMetrics());
        }

        bucketKey = BucketCalculator.getCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(String metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 2L, testTrackingDataDownload.getLongMetrics());
        }

        bucketKey = BucketCalculator.getCalendar(2014, Calendar.AUGUST, 1, 0, 0, 0, timeZonePST).getTimeInMillis();
        for(String metric : allMetrics) {
            verifyValueForMetric(metric, bucketKey, 233L, testTrackingDataDownload.getLongMetrics());
        }

        // There should be only results for 3 months

        for(String metric : allMetrics) {
            Assert.assertEquals(3, testTrackingDataDownload.getLongMetrics().get(metric).size());
        }
    }


    @Test
    public void testGetAggregatedMetricForDevice_year_aggregation() {

        String trackingDeviceId = createTrackingDeviceId();

        //
        // Do the first post followed by a GET to validate aggregated data
        //

        TestTrackingDataUpload testTrackingData1 = BucketCalculator.generateRandomLongTrackingData(
                BucketCalculator.getCalendar(2012, Calendar.MAY, 7, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2012, Calendar.MAY, 20, 0, 0, 0, timeZonePST));

        TestTrackingDataUpload testTrackingData2 = BucketCalculator.generateRandomLongTrackingData(
                BucketCalculator.getCalendar(2014, Calendar.FEBRUARY, 2, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2014, Calendar.MARCH, 3, 0, 0, 0, timeZonePST));

        TestTrackingDataUpload combinedTestTrackingData1 = BucketCalculator.combineLongTrackingData(testTrackingData1, testTrackingData2);

        ResponseEntity<String> responseData1 = postMetricsForDevice(trackingDeviceId, combinedTestTrackingData1, timeZonePST);
        blockUntilAsyncThreadIdleInServer();

        TestTrackingDataDownload testTrackingDataDownload1 = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TestAggregationLevel.YEARS.toString(),
                2012,
                null,
                null,
                null,
                null,
                null,
                null,
                timeZonePST);

        Assert.assertEquals("Unexpected number of long metric types in get response",
                4, testTrackingDataDownload1.getLongMetrics().size());

        for(String testTrackingMetric : TrackingMetricConfigUtil.getAllLongTypeMetrics()) {

            Assert.assertEquals("Unexpected number of year buckets types in get response for metric " + testTrackingMetric,
                    2, testTrackingDataDownload1.getLongMetrics().get(testTrackingMetric).size());

            long firstYearExpectedValue = sumValuesFromLongMetric(testTrackingData1, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2012, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(),
                    firstYearExpectedValue, testTrackingDataDownload1.getLongMetrics());

            long secondYearExpectedValue = sumValuesFromLongMetric(testTrackingData2, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2014, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(),
                    secondYearExpectedValue, testTrackingDataDownload1.getLongMetrics());
        }

        //
        // Do a 2nd post and a GET to validate updated aggregated data
        //

        TestTrackingDataUpload testTrackingData3 = BucketCalculator.generateRandomLongTrackingData(
                BucketCalculator.getCalendar(2012, Calendar.MAY, 22, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2012, Calendar.MAY, 27, 0, 0, 0, timeZonePST));

        TestTrackingDataUpload testTrackingData4 = BucketCalculator.generateRandomLongTrackingData(
                BucketCalculator.getCalendar(2014, Calendar.AUGUST, 9, 0, 0, 0, timeZonePST),
                BucketCalculator.getCalendar(2014, Calendar.AUGUST, 23, 0, 0, 0, timeZonePST));

        TestTrackingDataUpload combinedTestTrackingData2 = BucketCalculator.combineLongTrackingData(testTrackingData3, testTrackingData4);
        ResponseEntity<String> responseData2 = postMetricsForDevice(trackingDeviceId, combinedTestTrackingData2, timeZonePST);
        blockUntilAsyncThreadIdleInServer();

        TestTrackingDataDownload testTrackingDataDownload2 = getAggregatedMetricsForDevice(
                trackingDeviceId,
                TestAggregationLevel.YEARS.toString(),
                2012,
                null,
                null,
                null,
                null,
                null,
                null,
                timeZonePST);

        Assert.assertEquals("Unexpected number of long metric types in 2nd get response",
                4, testTrackingDataDownload2.getLongMetrics().size());

        for(String testTrackingMetric : TrackingMetricConfigUtil.getAllLongTypeMetrics()) {

            Assert.assertEquals("Unexpected number of year buckets types in 2nd get response for metric " + testTrackingMetric,
                    2, testTrackingDataDownload2.getLongMetrics().get(testTrackingMetric).size());

            long firstYearExpectedValue = sumValuesFromLongMetric(testTrackingData1, testTrackingMetric);
            firstYearExpectedValue = firstYearExpectedValue + sumValuesFromLongMetric(testTrackingData3, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2012, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(),
                    firstYearExpectedValue, testTrackingDataDownload2.getLongMetrics());

            long secondYearExpectedValue = sumValuesFromLongMetric(testTrackingData2, testTrackingMetric);
            secondYearExpectedValue = secondYearExpectedValue + sumValuesFromLongMetric(testTrackingData4, testTrackingMetric);
            verifyValueForMetric(testTrackingMetric, BucketCalculator.getCalendar(2014, Calendar.JANUARY, 1, 0, 0, 0, timeZonePST).getTimeInMillis(),
                    secondYearExpectedValue, testTrackingDataDownload2.getLongMetrics());
        }
    }



    //@Test
    public void metricTest() {

        String trackingDeviceId = "blablabla";

        TestTrackingDataUpload testTrackingData = new TestTrackingDataUpload();

        TreeMap<Long, Long> dataPoints = new TreeMap<Long, Long>();

        // Data point 1: May 29th, 2014, PST
        long timeStamp1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 29, 0, 0, 0, timeZonePST).getTimeInMillis();
        dataPoints.put(timeStamp1, 4L);

        BucketCalculator.addDataPointForAllLongMetrics(testTrackingData, dataPoints);

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



    //
    // Utility methods
    //

    private long sumValuesFromLongMetric(TestTrackingDataUpload testTrackingData, String testTrackingMetric) {

        Map<Long, Long> mapToSumValuesFrom = testTrackingData.getLongMetrics().get(testTrackingMetric);

        long sum = 0;

        for(long value : mapToSumValuesFrom.values()) {
            sum = sum + value;
        }

        return sum;
    }

    private void verifyValueForMetric(
            String testTrackingMetric,
            Long bucketKey,
            Long expectedBucketValue,
            Map<String, TreeMap<Long, Long>> metricResponse) {

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
