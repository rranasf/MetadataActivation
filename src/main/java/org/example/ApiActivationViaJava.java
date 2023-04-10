package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import java.io.*;
import java.util.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

class HttpRequest {
    String ExecuteHttpGet(String url) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            HttpGet request = new HttpGet(url);

            HttpResponse response = client.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                try (InputStream stream = entity.getContent()) {
                    BufferedReader reader =
                            new BufferedReader(new InputStreamReader(stream));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }
                    return reader.toString();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";

    }
}


public class ApiActivationViaJava {
    private static UDF1 executeRestApiUDF = new UDF1<String, String>() {
        public String call(final String url) throws Exception {
            HttpRequest httpRequest = new HttpRequest();
            return httpRequest.ExecuteHttpGet(url);
        }
    };

    public static void main(String[] args) {
        String appName = "main-app-test";
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName(appName)
                .getOrCreate();


        spark.udf().register("executeRestApiUDF", executeRestApiUDF, DataTypes.StringType);

        Dataset<Row> restCallsDataSet = spark.createDataFrame(Arrays.asList(
                new RestAPIUrl("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"),
                new RestAPIUrl("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json")), RestAPIUrl.class);

        restCallsDataSet.show(5);

        List<StructField> reportFields = new ArrayList<StructField>();
        reportFields.add(DataTypes.createStructField("Count", DataTypes.IntegerType, true));
        reportFields.add(DataTypes.createStructField("Message", DataTypes.StringType, true));
        reportFields.add(DataTypes.createStructField("SearchCriteria", DataTypes.StringType, true));

        restCallsDataSet = restCallsDataSet.withColumn("result", callUDF("executeRestApiUDF", col("url")));
        restCallsDataSet.show(5);

    }
}

//        val c = udf(new UDF1[String, String] {
//        override def call(url: String) = {
//        val httpRequest = new HttpRequest;
//        httpRequest.ExecuteHttpGet(url).getOrElse("")
//        }
//        }, StringType)

// Lets set up an example test
// create the Dataframe to bind the UDF to
//        case class RestAPIRequest (url: String)

// We will put our DF here.
// We will take the data parameters from the Metadataformat defined and form the REST API.
// Depending on what method is configured for the partner (GET/POST), the request will be formed.
// corresponding http call will be made in executeRestApiUDF.
// Source is CSV

// CSV => DF

// CSV to JSON(Payload)
// PUT req with JSON payload for each record.


// google.dbm.activation.com/u=abc@gmai.com/segments=avbff,efrfr,frfrfr
// GET google.dbm.activation.com/u=%s/segments=%s

////  val restApiCallsToMake = Seq(RestAPIRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"),
//        RestAPIRequest("https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json"))
//        val source_df = restApiCallsToMake.toDF()
//        source_df.show(5)
//
//        // Define the schema used to format the REST response.  This will be used by from_json
//        val restApiSchema = StructType(List(
//        StructField("Count", IntegerType, true),
//        StructField("Message", StringType, true),
//        StructField("SearchCriteria", StringType, true),
//        StructField("Results", ArrayType(
//        StructType(List(
//        StructField("Make_ID", IntegerType, true),
//        StructField("Make_Name", StringType, true)
//        ))
//        ), true)
//        ))
//
//        // add the UDF column, and a column to parse the output to
//        // a structure that we can interogate on the dataframe
//        val execute_df = source_df
//        .withColumn("result", executeRestApiUDF(col("url")))
//        .withColumn("result", from_json(col("result"), restApiSchema))
//
//        execute_df.show(20)
//
//        // call an action on the Dataframe to execute the UDF
//        // process the results
//        execute_df.select(explode(col("result.Results")).alias("makes"))
//        .select(col("makes.Make_ID"), col("makes.Make_Name"))
//        .show
//        }