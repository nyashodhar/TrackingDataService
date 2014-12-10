package com.petpal.tracking.integration;

import com.petpal.tracking.TrackingDataServiceConfiguration;
import com.petpal.tracking.util.JSONUtil;
import org.apache.log4j.Logger;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 12/8/14.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TrackingDataServiceConfiguration.class)
@WebAppConfiguration
@IntegrationTest({"server.port:0","management.port:0"})   // Will start the server on a random port
public class SerializeTestIntegrationTest extends AbstractTimeSeriesIntegrationTest {

    private Logger logger = Logger.getLogger(this.getClass());

    private TimeZone timeZonePST;

    @Value("${local.server.port}")
    protected int port;

    @Before
    public void setup() {
        logger.info("SerializeTestIntegrationTest.setup(): port = " + port);
        timeZonePST = TimeZone.getTimeZone("PST");
    }

    @Test
    public void testSerialization_long() {

        TreeMap<Long, Long> seriesMap = new TreeMap<Long, Long>();
        seriesMap.put(123L, new Long(12L));
        seriesMap.put(124L, new Long(13L));
        seriesMap.put(125L, new Long(Long.MAX_VALUE));

        Map<String, TreeMap<Long, Long>> longSeries = new HashMap<String, TreeMap<Long, Long>>();
        longSeries.put("WALKINGSTEPS", seriesMap);

        TestTrackingDataTest testTrackingDataTest = new TestTrackingDataTest();
        testTrackingDataTest.setLongMetrics(longSeries);

        postMetricsForDeviceTEST("abc", testTrackingDataTest, timeZonePST);
    }


    protected ResponseEntity<String> postMetricsForDeviceTEST(
            String deviceId, TestTrackingDataTest testTrackingDataTest, TimeZone aggregationTimeZone) {
            //String deviceId, Map<String, TreeMap<Long, Long>> testTrackingData, TimeZone aggregationTimeZone) {

        String json = JSONUtil.convertToString(testTrackingDataTest);
        //System.out.println("*** Tracking data: " + testTrackingDataTest);
        System.out.println("*** testTrackingDataTest json: " + json);

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL.APPLICATION_JSON));
        headers.setContentType(MediaType.ALL.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(json, headers);
        String url = "http://localhost:" + port + "/tracking/TEST/device/" + deviceId;
        //String url = "http://localhost:" + port + "/tracking/TEST";

        if(aggregationTimeZone != null) {
            url = url + "?aggregationTimeZone=" + aggregationTimeZone.getID();
        }

        try {
            logger.info("postMetricsForDeviceTEST(): doing post to " + url);
            long start = System.currentTimeMillis();
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            long end = System.currentTimeMillis();
            logger.info("postMetricsForDeviceTEST(): response received in " + (end - start) + " ms.");

            try {
                Thread.sleep(KAIROS_WRITE_DELAY);
            } catch(InterruptedException e) {
                logger.error("postMetricsForDeviceTEST(): Unexpected interrupted exception", e);
            }

            return response;
        } catch(RestClientException e) {
            if(e instanceof HttpServerErrorException) {
                logger.error("postMetricsForDeviceTEST(): Unexpected server error: " +
                        ((HttpServerErrorException) e).getStatusCode() + ", body: " +
                        ((HttpClientErrorException) e).getResponseBodyAsString(), e);
            } else if(e instanceof HttpClientErrorException) {
                logger.error("postMetricsForDeviceTEST(): Unexpected client error: " +
                        ((HttpClientErrorException) e).getStatusCode() + ", body: " +
                        ((HttpClientErrorException) e).getResponseBodyAsString(), e);
            } else {
                logger.error("postMetricsForDeviceTEST(): Unexpected error: " + ((HttpClientErrorException) e).getStatusCode(), e);
            }
            throw e;
        }
    }


    public class TestTrackingDataTest {

        private Map<String, TreeMap<Long, Long>> longMetrics;
        private Map<String, TreeMap<Long, Long>> doubleMetrics;
        private Map<String, TreeMap<Long, Long>> stringMetrics;

        public void setLongMetrics(Map<String, TreeMap<Long, Long>> longMetrics) {
            this.longMetrics = longMetrics;
        }

        public void setDoubleMetrics(Map<String, TreeMap<Long, Long>> doubleMetrics) {
            this.doubleMetrics = doubleMetrics;
        }

        public void setStringMetrics(Map<String, TreeMap<Long, Long>> stringMetrics) {
            this.stringMetrics = stringMetrics;
        }

        public Map<String, TreeMap<Long, Long>> getLongMetrics() {
            return longMetrics;
        }

        public Map<String, TreeMap<Long, Long>> getDoubleMetrics() {
            return doubleMetrics;
        }

        public Map<String, TreeMap<Long, Long>> getStringMetrics() {
            return stringMetrics;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("TestTrackingDataTest{");
            sb.append("longMetrics=").append(longMetrics);
            sb.append(", doubleMetrics=").append(doubleMetrics);
            sb.append(", stringMetrics=").append(stringMetrics);
            sb.append('}');
            return sb.toString();
        }
    }





}
