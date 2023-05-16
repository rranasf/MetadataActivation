package org.example;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class PartnerConfigReader {
    private static InputStream inputStreamFromClasspath( String path ) {

        // returning stream
        return Thread.currentThread().getContextClassLoader().getResourceAsStream( path );
    }

    public PartnerConfig loadPartnerConfig(String filePath) {
        PartnerConfig partnerConfig = null;
        try {
            JSONParser jsonParser = new JSONParser();
            InputStream inputStream = inputStreamFromClasspath(filePath);

            JSONObject jsonObject = (JSONObject) jsonParser.parse(
                    new InputStreamReader(inputStream, "UTF-8"));


            ObjectMapper om = new ObjectMapper();
            partnerConfig = om.readValue(jsonObject.toString(), PartnerConfig.class);
            return partnerConfig;
        } catch (Exception e) {
            System.out.println("Caught Exception: "+e);
        }
        return partnerConfig;
    }

    public boolean validateConfigAgainstSchema() throws Exception {

        // create instance of the ObjectMapper class
        ObjectMapper objectMapper = new ObjectMapper();

        // create an instance of the JsonSchemaFactory using version flag
        JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance( SpecVersion.VersionFlag.V201909 );

        // store the JSON data in InputStream
        try(
                InputStream jsonStream = inputStreamFromClasspath("testPartnerConfig.json");
                InputStream schemaStream = inputStreamFromClasspath("ApiPartnerConfigSchema.json")

        ) {
            // read data from the stream and store it into JsonNode
            JsonNode json = objectMapper.readTree(jsonStream);

            // get schema from the schemaStream and store it into JsonSchema
            JsonSchema schema = schemaFactory.getSchema(schemaStream);

            System.out.println(schema.toString());

            // create set of validation message and store result in it
            Set<ValidationMessage> validationResult = schema.validate( json );
            System.out.println(validationResult.toString());

            // show the validation errors
            if (validationResult.isEmpty()) {

                // show custom message if there is no validation error
                System.out.println( "There is no validation errors" );
                return true;

            } else {

                // show all the validation error
                validationResult.forEach(vm -> System.out.println(vm.getMessage()));
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PartnerConfigReader reader = new PartnerConfigReader();
//        reader.validateJsonAgainstSchema();
        reader.loadPartnerConfig("testPartnerConfig.json");
    }


}
