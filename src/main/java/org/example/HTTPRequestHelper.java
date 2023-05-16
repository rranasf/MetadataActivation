package org.example;

import com.github.rholder.retry.*;
import com.google.common.base.Predicates;
import com.google.gson.Gson;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class HTTPRequestHelper {

    public HttpRequest getHTTPPostRequest(PartnerConfig partnerConfig, final List<String> batchData) {
        String hostName = partnerConfig.connectionProperties.serverAddress;
        int  port = partnerConfig.connectionProperties.serverPort;
        String httpPrefix = partnerConfig.connectionProperties.isSSL ? "https" : "http";
        int numberOfMaximumConnections = Integer.valueOf(partnerConfig.connectionProperties.numberOfMaximumConnections);

        String urlPath = partnerConfig.apiProperties.urlPath.isEmpty() ? "" : partnerConfig.apiProperties.urlPath;

        List<QueryParameter> qps = partnerConfig.apiProperties.queryParameters;
        List<Header> headers = partnerConfig.apiProperties.headers;
        String method =  partnerConfig.apiProperties.httpMethod;
        try {
            if (method.equalsIgnoreCase("POST")) {
                URIBuilder builder = new URIBuilder();
                builder.setScheme(httpPrefix);
                builder.setHost(hostName);
                builder.setPort(port);

                builder.setPath(urlPath);
                if (qps != null && qps.size() > 0) {
                    for (QueryParameter qp : qps) {
                        builder.addParameter(qp.name, qp.value);
                    }
                }
                HttpPost postRequest = new HttpPost(builder.build().toURL().toURI());

                StringEntity params = new StringEntity( new Gson().toJson(batchData));

//                StringEntity params = new StringEntity(jsonData);
                postRequest.addHeader("content-type", "application/json");
                for (Header header : headers) {
                    postRequest.addHeader(header.name, header.value);
                }
                System.out.println(postRequest.toString());
                System.out.println(postRequest.getAllHeaders().toString());
                postRequest.setEntity(params);

                return  postRequest;
            }
        }catch(Exception e) {
            System.out.println("Exception Caught while creating HTTP Request from payload Data: "+e);
        }
        return null;
    }


    public void executeHTTPPostForBatch(HttpClient httpClient, PartnerConfig partnerConfig, List<String> batch) {
        HttpRequest request = getHTTPPostRequest(partnerConfig, batch);
        if (request != null) {
            HttpResponse response = null;
            Callable<HttpResponse> callable = new Callable<HttpResponse>() {
                public HttpResponse call() throws Exception {
//                    HttpClient client = new DefaultHttpClient();
                    HttpResponse response = httpClient.execute((HttpPost)request);
                    if (response.getStatusLine().getStatusCode() != 200) {
                        EntityUtils.consume(response.getEntity());
                        return null;
                    }
                    EntityUtils.consume(response.getEntity());
                    return response;
                }
            };

            // Retry mechanism
            Retryer<HttpResponse> retryer = RetryerBuilder.<HttpResponse>newBuilder()
                    .retryIfResult(Predicates.isNull())
                    .retryIfException()
                    .withWaitStrategy(WaitStrategies.exponentialWait(1000, 100000, TimeUnit.SECONDS))
//                    .withWaitStrategy(WaitStrategies.fixedWait(500, TimeUnit.MILLISECONDS))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(6))
                    .build();

            try {
                response = retryer.call(callable);
            } catch (RetryException e) {
                System.out.println("Failure: API returned Failure.");
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            if (response != null) {
                System.out.println("Success: API returned success.");
                // return String.valueOf(response.getStatusLine()); //Get the data in the entity
            }
        }
    }




}
