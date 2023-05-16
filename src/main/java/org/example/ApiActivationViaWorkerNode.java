package org.example;

import com.google.common.util.concurrent.RateLimiter;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.commons.cli.*;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.json.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class ApiActivationViaWorkerNode {

    public static PartnerConfigFactory partnerConfigFactory = new PartnerConfigFactory();
    public static HTTPRequestHelper httpRequestHelper = new HTTPRequestHelper();

    public static RateLimiter rateLimiter = null;

    public static CloseableHttpClient httpClient = null;

    public static PartnerConfig partnerConfig = null;

    public static class WorkerThread implements Runnable {

        String fileName;
        PartnerConfig partnerConfig;

        List<String> headers;

        public WorkerThread(String s, PartnerConfig partnerConfig, List<String> headers) {
            this.fileName = s;
            this.partnerConfig = partnerConfig;
            this.headers = headers;
        }

        public void run() {
            System.out.println(Thread.currentThread().getName() + " (Start) Filename = " + fileName);
            processFile(); // call processFile method that processes the given file
            System.out.println(Thread.currentThread().getName() + " (End)"); //prints thread name
        }

        private void processFile() {
            List<String[]> lines = new ArrayList<>();
            try {
                CSVReader csvReader = new CSVReaderBuilder(new FileReader(fileName)).withSkipLines(1).build();
                String[] line;
                while (( line = csvReader.readNext()) != null) {
                    lines.add(line);
                }
            } catch (Exception e) {
                System.out.println("Execption reading the CSV file" + e);
            }


            int BATCH_SIZE = partnerConfig.getBatchSize();

            List<String> batch = new ArrayList<>(BATCH_SIZE);
            for (String[] line: lines) {

                JSONObject jo = new JSONObject();
                for (int i = 0; i < headers.size(); i++) {
                    CsvToJsonParser.deepenJsonWithDotNotation(jo, headers.get(i), line[i]);
                }
                String payload = jo.toString();

                batch.add(payload);

                if (batch.size() == BATCH_SIZE) {
//                    rateLimiter.acquire(1);
                    httpRequestHelper.executeHTTPPostForBatch(httpClient, partnerConfig, batch);
                    batch.clear();
                }
            }
            if (batch.size() > 0){
//                rateLimiter.acquire(1);
                httpRequestHelper.executeHTTPPostForBatch(httpClient, partnerConfig, batch);
            }
        }
    }


    public static void main(String[] args) {
        // Read the commandline Argument for File Path and partnerName
        Options options = new Options();
        Option filePath = new Option("f", "filePath", true, "input file path");
        filePath.setRequired(true);
        options.addOption(filePath);

        Option s3Path = new Option("s", "filePath", true, "input file path");
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
        String inputFolderPath = cmd.getOptionValue("filePath");

        File folderToRead = new File(inputFolderPath);
        File[] listOfFiles = folderToRead.listFiles();
        String partnerName = cmd.getOptionValue("partner");

        partnerConfig = partnerConfigFactory.getPartnerConfig(partnerName);

        rateLimiter = RateLimiter.create(partnerConfig.getMaximumRequestsPerMinute());
        int numParallelConnections = partnerConfig.getMaximumParallelConnections();
        ExecutorService executorService = Executors.newFixedThreadPool(numParallelConnections);

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(numParallelConnections);

         httpClient
                = HttpClients.custom().setConnectionManager(connectionManager)
                .build();



        List<String> filesToSend = new ArrayList<>();
//                inputFilePaths.split(",");
        for (File inputFile: listOfFiles) {
            filesToSend.add(inputFile.getPath());
        }

        List<String> headers = CsvToJsonParser.readHeaders(filesToSend.get(0));

        // Add all other files here
        // If we have to read from S3, add all the keynames here.
        List<WorkerThread> workerThreads = new ArrayList<>();
        for(String fileName: filesToSend) {
            workerThreads.add(new WorkerThread(fileName, partnerConfig, headers));
        }
        for (int i = 0; i<filesToSend.size(); i++) {
            executorService.execute(workerThreads.get(i));
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                System.out.println("Executor service Completed successfully");
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            System.out.println("Executor service did not finish "+ ex);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

    }

}



