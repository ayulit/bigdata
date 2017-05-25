package ru.mera.practice;

import java.io.Serializable;
import java.lang.String;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents a logins assignment log line (model).
 * See src/main/resources/task07.txt as a log example.
 */
public class LoginAssignmentLog implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger("LoginAssignment");

    private String ipAddress;      // 1:IP        - 127.0.0.1
    private String clientIdentd;   // 2:client    - 
    private String userID;         // 3:user      - luke
    private String dateTimeString; // 4:date time - [21/Jul/2014:9:55:27 -0800]
    private String method;         // 5:method    - GET
    private String endpoint;       // 6:req       - /home.html
    private String protocol;       // 7:proto     - HTTP/1.1
    private int responseCode;      // 8:respcode  - 200
    private long contentSize;      // 9:size      - 2048

    private LoginAssignmentLog(String ipAddress, String clientIdentd, String userID,
            String dateTime, String method, String endpoint,
            String protocol, String responseCode,
            String contentSize) {
        this.ipAddress = ipAddress;
        this.clientIdentd = clientIdentd;
        this.userID = userID;
        this.dateTimeString = dateTime;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize = Long.parseLong(contentSize);
    }

    public String getIpAddress() {
        return ipAddress;
    }
    public String getClientIdentd() {
        return clientIdentd;
    }
    public String getUserID() {
        return userID;
    }
    public String getDateTimeString() {
        return dateTimeString;
    }
    public String getMethod() {
        return method;
    }
    public String getEndpoint() {
        return endpoint;
    }
    public String getProtocol() {
        return protocol;
    }
    public int getResponseCode() {
        return responseCode;
    }
    public long getContentSize() {
        return contentSize;
    }
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
    public void setClientIdentd(String clientIdentd) {
        this.clientIdentd = clientIdentd;
    }
    public void setUserID(String userID) {
        this.userID = userID;
    }
    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }
    public void setMethod(String method) {
        this.method = method;
    }
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }
    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    // Example Apache log line:
    //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static LoginAssignmentLog parseFromLogLine(String logline) {
        
        Matcher m = PATTERN.matcher(logline);
        
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new LoginAssignmentLog(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
    }

    @Override
    public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s",
                ipAddress, clientIdentd, userID, dateTimeString, method, endpoint,
                protocol, responseCode, contentSize);
    }
}