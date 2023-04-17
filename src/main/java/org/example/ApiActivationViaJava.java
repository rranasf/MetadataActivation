package org.example;

import com.github.rholder.retry.*;

import com.google.common.base.Predicates;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.*;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

//class HttpRequest {
//    String ExecuteHttpGet(String url) {
//        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
//            HttpGet request = new HttpGet(url);
//
//            HttpResponse response = client.execute(request);
//            HttpEntity entity = response.getEntity();
//            if (entity != null) {
//                try (InputStream stream = entity.getContent()) {
//                    BufferedReader reader =
//                            new BufferedReader(new InputStreamReader(stream));
//                    return  reader.lines().collect(Collectors.joining());
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return "";
//
//    }
//}


public class ApiActivationViaJava {

    public static PartnerConfig partnerConfig = new PartnerConfigReader().loadJson();

    public static HttpRequest getHTTPRequest(final String jsonData) {
        // Creating the HTTP Request from partner configs
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
                StringEntity params = new StringEntity(jsonData);
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

    private static UDF1 executeRestApiUDF = new UDF1<String , String>() {
        public String call(final String jsonData) throws Exception {

            HttpRequest request = getHTTPRequest(jsonData);
//            RateLimiter rateLimiter = RateLimiter.create(2);

            if (request != null) {
                HttpResponse response = null;
                Callable<HttpResponse> callable = new Callable<HttpResponse>() {
                    public HttpResponse call() throws Exception {
                        HttpClient client = new DefaultHttpClient();
                        HttpResponse response = client.execute((HttpPost)request);
                        if (response.getStatusLine().getStatusCode() != 200) {
                            return null;
                        }
                        return response;
                    }
                };

                // Retry mechanism
                Retryer<HttpResponse> retryer = RetryerBuilder.<HttpResponse>newBuilder()
                        .retryIfResult(Predicates.isNull())
                        .retryIfException()
                        .withWaitStrategy(WaitStrategies.fixedWait(500, TimeUnit.MILLISECONDS))
                        .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                        .build();
                try {
                    response = retryer.call(callable);
                } catch (RetryException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                if (response != null) {
                    return String.valueOf(response.getStatusLine()); //Get the data in the entity
                }
            }
            return "";
        }

    };


    public static void main(String[] args) throws Exception {

//        JSONArray jsonArray = new JSONArray();
        String filePath = "/Users/r.rana/work/nestedSample.csv";

        List<String> headers = CsvToJsonParser.readHeaders(filePath);

        String appName = "main-app-test";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName(appName)
                .getOrCreate();


        Dataset<Row> df = spark.read().option("mode", "DROPMALFORMED")
                .option("header", "true")
                .csv("/Users/r.rana/work/nestedSample.csv");

        Encoder<String> stringEncoder = Encoders.STRING();


        Dataset<Row> jsonDF = df.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                JSONObject jo = new JSONObject();

                for (int i = 0; i < headers.size(); i++) {
                    CsvToJsonParser.deepenJsonWithDotNotation(jo, headers.get(i), row.getString(i));
                }
                return jo.toString();
            }
        }, stringEncoder).toDF("jsonValue");



        jsonDF.show(5);

        spark.udf().register("executeRestApiUDF", executeRestApiUDF, DataTypes.StringType);
        System.out.println("Executing the REST calls");

        jsonDF = jsonDF.withColumn("result", callUDF("executeRestApiUDF", col("jsonValue")));
        jsonDF.show(5);

    }
}