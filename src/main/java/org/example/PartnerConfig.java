package org.example;

import java.util.*;

// import com.fasterxml.jackson.databind.ObjectMapper; // version 2.11.1
// import com.fasterxml.jackson.annotation.JsonProperty; // version 2.11.1
/* ObjectMapper om = new ObjectMapper();
Root root = om.readValue(myJsonString, Root.class); */
class ApiProperties{
    public String urlPath;
    public ArrayList<QueryParameter> queryParameters;
    public ArrayList<Header> headers;
    public String httpMethod;
    public String payloadType;
    public OperationalProperties operationalProperties;
    public TimeoutProperties timeoutProperties;
}

class ConnectionProperties{
    public String serverAddress;
    public int serverPort;
    public boolean isSSL;
    public int numberOfMaximumConnections;
}

class Header{
    public String value;
    public String name;
}

class OperationalProperties{
    public int retryCount;
    public int maxNumberOfRequestsPerMin;
    public int payloadSizeLimit;
}

class QueryParameter{
    public String value;
    public String name;
}

public class PartnerConfig{
    public String connectorType;

    public String getConnectorType() {
        return connectorType;
    }
    public ConnectionProperties connectionProperties;
    public ApiProperties apiProperties;
}

class TimeoutProperties{
    public int readTimeout;
    public int connectionTimeout;
}


