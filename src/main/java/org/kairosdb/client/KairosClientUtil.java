package org.kairosdb.client;

import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Wrap some methods in the kairos rest client to prevent them from throwing
 * checked exceptions.
 *
 * Created by per on 10/30/14.
 */
public class KairosClientUtil {

    public static QueryResponse executeQuery(QueryBuilder queryBuilder, KairosRestClient kairosRestClient) {

        QueryResponse queryResponse;

        try {
            queryResponse = kairosRestClient.query(queryBuilder);
        } catch(IOException e) {
            throw new RuntimeException("IOException when doing query, kairosDBHost=", e);
        } catch(URISyntaxException e) {
            throw new RuntimeException("URISyntaxException when doing query", e);
        }

        return queryResponse;
    }

    public static Response pushMetrics(MetricBuilder metricBuilder, KairosRestClient kairosRestClient) {

        Response response;

        try {
            response = kairosRestClient.pushMetrics(metricBuilder);
        } catch(IOException e) {
            throw new RuntimeException("IOException when pushing metrics", e);
        } catch(URISyntaxException e) {
            throw new RuntimeException("URISyntaxException when pushing metrics", e);
        }

        return response;
    }

    public static long getLongValueFromDataPoint(DataPoint dataPoint) {
        try {
            return dataPoint.longValue();
        } catch(DataFormatException e) {
            throw new RuntimeException("Unable to get long value from data point, dataPoint.getValue() = " + dataPoint.getValue());
        }
    }
}
