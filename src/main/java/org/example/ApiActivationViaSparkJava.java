package org.example;

import com.github.rholder.retry.*;
import com.google.common.base.Predicates;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.*;
import org.apache.http.HttpResponse;
import org.json.JSONObject;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.commons.cli.*;

public class ApiActivationViaSparkJava {

        public static PartnerConfigFactory partnerConfigFactory = new PartnerConfigFactory();
        public static HTTPRequestHelper httpRequestHelper = new HTTPRequestHelper();

    private static UDF2 executeRestApiUDF = new UDF2<String, PartnerConfig, String>() {
        public String call(final String jsonData, PartnerConfig partnerConfig) throws Exception {

            HttpRequest request = httpRequestHelper.getHTTPPostRequest(partnerConfig, new ArrayList<>(Arrays.asList(jsonData)));

            RateLimiter rateLimiter = RateLimiter.create(2);

            if (request != null) {
                HttpResponse response = null;
                Callable<HttpResponse> callable = new Callable<HttpResponse>() {
                    public HttpResponse call() throws Exception {
                        HttpClient client = new DefaultHttpClient();
                        rateLimiter.acquire(1);
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


    public static void callAPIForRows(PartnerConfig partnerConfig, int perPartitionRateLimit, Iterator<Row> it) {

        int BATCH_SIZE = partnerConfig.getBatchSize();
        RateLimiter rateLimiter = RateLimiter.create(perPartitionRateLimit);
        List<String> batch = new ArrayList<>(BATCH_SIZE);
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(1);

        HttpClient httpClient
                = HttpClients.custom().setConnectionManager(connectionManager)
                .build();

        while (it.hasNext()) {
            Row row = it.next();
            batch.add(row.getString(0));
            if (batch.size() == BATCH_SIZE) {
                rateLimiter.acquire(1);
                httpRequestHelper.executeHTTPPostForBatch(httpClient, partnerConfig, batch);
                batch.clear();
            }
        }
        if (batch.size() > 0){
            rateLimiter.acquire(1);
            httpRequestHelper.executeHTTPPostForBatch(httpClient, partnerConfig, batch);
        }
    }
    public static void main(String[] args) throws Exception {

        // Read the commandline Argument for File Path and partnerName
        Options options = new Options();
        Option filePath = new Option("f", "filePath", true, "input file path");
        filePath.setRequired(true);
        options.addOption(filePath);

        Option partner = new Option("p", "partner", true, "Activation Partner");
        filePath.setRequired(true);
        options.addOption(partner);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        String inputFilePath = cmd.getOptionValue("filePath");
        String partnerName = cmd.getOptionValue("partner");

        PartnerConfig partnerConfig = partnerConfigFactory.getPartnerConfig(partnerName);

        List<String> headers = CsvToJsonParser.readHeaders(inputFilePath);

        String appName = "main-app-test";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName(appName)
                .getOrCreate();


        Dataset<Row> ds = spark.read().option("mode", "DROPMALFORMED")
                .option("header", "true")
                .csv(inputFilePath);

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<Row> jsonDS = ds.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                JSONObject jo = new JSONObject();

                for (int i = 0; i < headers.size(); i++) {
                    CsvToJsonParser.deepenJsonWithDotNotation(jo, headers.get(i), row.getString(i));
                }
                return jo.toString();
            }
        }, stringEncoder).toDF("jsonValue");


        jsonDS.show(5);

//        spark.udf().register("executeRestApiUDF", executeRestApiUDF, DataTypes.StringType);

        System.out.println("Executing the REST calls");
        System.out.println("number of partitions before setting: "+ jsonDS.rdd().getNumPartitions());

        int numPartitions = partnerConfig.getMaximumParallelConnections();

        // Re-partition the Data
        jsonDS.repartition(numPartitions);
        System.out.println("number of partitions After Re-partition: "+ jsonDS.rdd().getNumPartitions());



        int overallRateLimit = partnerConfig.getMaximumRequestsPerMinute();
        int perPartitionRateLimit = overallRateLimit/numPartitions;

        jsonDS.javaRDD().foreachPartition( iter -> {
            callAPIForRows(partnerConfig, perPartitionRateLimit, iter);
        });


//        jsonDF = jsonDF.withColumn("result", callUDF("executeRestApiUDF", col("jsonValue"), partnerConfig ));
//        jsonDF.show(5);

    }
}