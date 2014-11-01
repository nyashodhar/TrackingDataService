package org.kairosdb.client;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.net.MalformedURLException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * HTTP implementation of a client.
 *
 * I am reimplementing this class to better control the configuration of the http client,
 * and also to fix a bad bug related to retries in the HttpClient class in the kairosdb-client library.
 */
public class KairosRestClient extends AbstractClient {

    @Value("${httpClient.maxConnectionsPerRoute}")
    private int httpClientMaxConnectionsPerRoute;

    @Value("${httpClient.maxConnectionsTotal}")
    private int httpClientMaxConnectionsTotal;

    private CloseableHttpClient client;
    private int retries = 3;

    private KairosRestClient httpClient;


    /**
     * Creates a client to talk to the host on the specified port.
     *
     * @param url url to KairosDB server
     */
    public KairosRestClient(String url) throws MalformedURLException {
        super(url);
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setMaxConnPerRoute(httpClientMaxConnectionsPerRoute);
        httpClientBuilder.setMaxConnTotal(httpClientMaxConnectionsTotal);
        client = httpClientBuilder.build();
    }

    @Override
    protected ClientResponse postData(String json, String url) throws IOException
    {
        StringEntity requestEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
        HttpPost postMethod = new HttpPost(url);
        postMethod.setEntity(requestEntity);

        return execute(postMethod);
    }

    @Override
    protected ClientResponse queryData(String url) throws IOException
    {
        HttpGet getMethod = new HttpGet(url);
        getMethod.addHeader("accept", "application/json");

        return execute(getMethod);
    }

    @Override
    protected ClientResponse delete(String url) throws IOException
    {
        HttpDelete deleteMethod = new HttpDelete(url);
        deleteMethod.addHeader("accept", "application/json");

        return execute(deleteMethod);
    }

    private ClientResponse execute(HttpUriRequest request) throws IOException
    {
        HttpResponse response;

        //int tries = ++retries;   <= BUG: number of tries keep on INCREASING!!
        int tries = retries;
        while (true)
        {
            tries--;
            try
            {
                response = client.execute(request);
                break;
            }
            catch (IOException e)
            {
                if (tries < 1)
                    throw e;
            }
        }

        return new HttpClientResponse(response);
    }

    @Override
    public void shutdown() throws IOException
    {
        client.close();
    }

    @Override
    public int getRetryCount()
    {
        return retries;
    }

    public void setRetryCount(int retries)
    {
        checkArgument(retries >= 0);
        this.retries = retries;
    }

    /**
     * Used for testing only
     */
    protected void setClient(CloseableHttpClient client)
    {
        this.client = client;
    }
}
