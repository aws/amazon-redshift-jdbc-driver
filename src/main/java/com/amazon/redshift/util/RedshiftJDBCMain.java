/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import com.amazon.redshift.Driver;

public class RedshiftJDBCMain {

  public static void main(String[] args) {

    java.net.URL url = Driver.class.getResource("/com/amazon/redshift/Driver.class");
    System.out.printf("%n%s%n", com.amazon.redshift.util.DriverInfo.DRIVER_FULL_NAME);
    System.out.printf("Found in: %s%n%n", url);

    System.out.printf("The Redshift JDBC driver is not an executable Java program.%n%n"
                       + "You must install it according to the JDBC driver installation "
                       + "instructions for your application / container / appserver, "
                       + "then use it by specifying a JDBC URL of the form %n    jdbc:redshift://%n"
                       + "or using an application specific method.%n%n"
                       + "See the Redshift JDBC documentation: https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html%n%n"
                       + "This command has had no effect.%n");

    System.exit(1);
  }
}
