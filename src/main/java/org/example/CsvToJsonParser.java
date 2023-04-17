package org.example;


import com.opencsv.*;
import org.jetbrains.annotations.NotNull;
import org.json.*;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.*;

public class CsvToJsonParser {
    private static String filePath = "/Users/r.rana/work/nestedSample.csv";

    public static void main(String[] args) {

        CsvToJsonParser parser = new CsvToJsonParser();
        System.out.println("Reading the input file at location: " + filePath);
        List<String> headers = parser.readHeaders(filePath);

        System.out.println("Headers are " + headers);

        JSONArray ja = parser.readCsvToJsonArray(filePath, headers);
        JSONObject mainObj = new JSONObject();
        mainObj.put("Individuals", ja);

        System.out.println("Converted json is: \n" + mainObj);

    }

    public static void deepenJsonWithDotNotation(JSONObject jsonObject, @NotNull String key, String value) {
        if (key.contains(".")) {
            String innerKey = key.substring(0, key.indexOf("."));
            String remaining = key.substring(key.indexOf(".") + 1);

            if (jsonObject.has(innerKey)) {
                deepenJsonWithDotNotation(jsonObject.getJSONObject(innerKey), remaining, value);
            } else {
                JSONObject innerJson = new JSONObject();
                jsonObject.put(innerKey, innerJson);
                deepenJsonWithDotNotation(innerJson, remaining, value);
            }
        } else {
            jsonObject.put(key, value);
        }
    }

    public JSONArray readCsvToJsonArray(String filePath, List<String> headers) {
        JSONArray jsonArray = new JSONArray();
        try {
            CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath)).withSkipLines(1).build();
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                if (headers.size() != nextRecord.length) {
                    System.out.println("Invalid record, skipping");
                } else {
                    JSONObject jo = new JSONObject();

                    for (int i = 0; i < headers.size(); i++) {
                        deepenJsonWithDotNotation(jo, headers.get(i), nextRecord[i]);
                    }
                    jsonArray.put(jo);
                }
            }

        } catch (Exception e) {
            System.out.println("caught Exception: " + e);
        }
        return jsonArray;
    }


    public static List<String> readHeaders(String filePath) {
        List<String> res = new ArrayList<>();
        try {
            File file = new File(filePath);

            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader);
            String[] nextRecord;
            nextRecord = csvReader.readNext();
            for (String cell : nextRecord) {
                res.add(cell);
            }


        } catch (Exception e) {
            System.out.println("Caught Exception while reading CSV file " + e);
        }
        return res;
    }
}