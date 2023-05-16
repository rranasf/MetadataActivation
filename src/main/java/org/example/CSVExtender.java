package org.example;
import com.opencsv.*;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

public class CSVExtender {
    public static void main(String[] args) {

        Options options = new Options();
        Option filePath = new Option("f", "filePath", true, "input file path");
        filePath.setRequired(true);
        options.addOption(filePath);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        String inputFilePath = cmd.getOptionValue("filePath");

        for (int j =0;j<100;j++) {

            String inputFilePathNew = inputFilePath.substring(0, inputFilePath.length()-4) + String.valueOf(j)+inputFilePath.substring(inputFilePath.length()-4, inputFilePath.length());
            File file = new File(inputFilePathNew);
            try {

                FileWriter outputfile = new FileWriter(file);
                CSVWriter writer = new CSVWriter(outputfile);
                String[] header = {"name.title", "name.firstName", "name.lastName", "email", "phoneNumber", "Address.firstLine", "Address.secondLine", "Address.thirdLine", "Address.StreetAddress.name", "Address.StreetAddress.number"};
                writer.writeNext(header);
                String[] titleStr = new String[2];
                titleStr[0] = "Mr.";
                titleStr[0] = "Ms.";
                List<String[]> data = new ArrayList<String[]>();
                for (int i = 0; i < 100000; i++) {
                    String randomStr = RandomStringUtils.randomAlphabetic(20);
                    String[] data1 = new String[header.length];

                    data1[0] = (i % 2 == 0) ? titleStr[0] : titleStr[1];
                    data1[1] = randomStr.substring(6, 10);
                    data1[2] = randomStr.substring(1, 7);
                    data1[3] = randomStr.substring(1, 10) + "@gmail.com";
                    data1[4] = RandomStringUtils.randomNumeric(10);
                    data1[5] = randomStr.substring(11, 16);
                    data1[6] = randomStr.substring(14, 19);
                    data1[7] = randomStr.substring(13, 17);
                    data1[8] = randomStr.substring(13, 17);
                    data1[9] = RandomStringUtils.randomNumeric(2);
                    data.add(data1);
//                writer.writeNext(data1);
                }
                writer.writeAll(data);
                writer.close();

            } catch (Exception e) {
                System.out.println("Exception caught: " + e);
            }
        }


    }
}
