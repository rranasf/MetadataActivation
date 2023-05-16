package org.example;

import java.io.Serializable;
import java.util.*;

// import com.fasterxml.jackson.databind.ObjectMapper; // version 2.11.1
// import com.fasterxml.jackson.annotation.JsonProperty; // version 2.11.1
/* ObjectMapper om = new ObjectMapper();
Root root = om.readValue(myJsonString, Root.class); */
class ApiProperties implements Serializable{
    public String urlPath;
    public ArrayList<QueryParameter> queryParameters;
    public ArrayList<Header> headers;
    public String httpMethod;
    public String payloadType;
    public OperationalProperties operationalProperties;
    public TimeoutProperties timeoutProperties;
}

class ConnectionProperties implements Serializable{
    public String serverAddress;
    public int serverPort;
    public boolean isSSL;
    public int numberOfMaximumConnections;
}

class Header implements Serializable{
    public String value;
    public String name;
}

class OperationalProperties implements Serializable{
    public int retryCount;
    public int maxNumberOfRequestsPerMin;
    public int payloadSizeLimit;
}

class QueryParameter implements Serializable{
    public String value;
    public String name;
}

public class PartnerConfig implements Serializable {

    public int getBatchSize() {
        return Math.min(this.apiProperties.operationalProperties.payloadSizeLimit/1000, 1000);
    }

    public int getMaximumParallelConnections() {
        return Integer.valueOf(this.connectionProperties.numberOfMaximumConnections);
    }

    public int getMaximumRequestsPerMinute() {
        return Math.min(Integer.valueOf(this.apiProperties.operationalProperties.maxNumberOfRequestsPerMin), 1000);
    }


    public String connectorType;

    public String getConnectorType() {
        return connectorType;
    }
    public ConnectionProperties connectionProperties;
    public ApiProperties apiProperties;
}

class TimeoutProperties implements Serializable{
    public int readTimeout;
    public int connectionTimeout;
}


