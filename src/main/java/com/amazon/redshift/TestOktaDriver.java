package com.amazon.redshift;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class TestOktaDriver {
    public static void main(String[] args) throws Exception {
        // Register the driver
        Class.forName("com.amazon.redshift.Driver");
        
        // Connection URL
        String url = "jdbc:redshift://hubble.chg6aanrjt24.eu-west-1.redshift.amazonaws.com:5439/dev";
        
        // Connection properties
        Properties props = new Properties();
        props.setProperty("plugin_name", "com.amazon.redshift.plugin.OktaRedshiftPlugin");
        props.setProperty("iamauth", "true");
        
        // Plugin parameters
        props.setProperty("ssoRoleName", "OktaDataViewer");
        props.setProperty("region", "eu-north-1");
        props.setProperty("ssoStartUrl", "https://d-c3672deb5f.awsapps.com/start");
        props.setProperty("preferred_role", "hubble-rbac/DataViewer");
        props.setProperty("ssoAccountID", "899945594626");
        props.setProperty("clusterid", "hubble");

        // Test connection
        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("Connected successfully!");
            
            // Test query
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT current_user, current_database()");
                if (rs.next()) {
                    System.out.println("Current user: " + rs.getString(1));
                    System.out.println("Current database: " + rs.getString(2));
                }
            }
        } catch (Exception e) {
            System.err.println("Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
