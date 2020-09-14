/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class with constants of Driver information.
 */
public final class DriverInfo {

  // Driver name
  public static final String DRIVER_NAME = "Redshift JDBC Driver";
  public static final String DRIVER_SHORT_NAME = "RsJDBC";
  public static final String DRIVER_VERSION;
  public static final String DRIVER_FULL_NAME;

  // Driver version
  public static final int MAJOR_VERSION;
  public static final int MINOR_VERSION;
  public static final int PATCH_VERSION;

  // JDBC specification
  public static final String JDBC_VERSION = "4.2";
  private static final int JDBC_INTVERSION = 42;
  public static final int JDBC_MAJOR_VERSION = JDBC_INTVERSION / 10;
  public static final int JDBC_MINOR_VERSION = JDBC_INTVERSION % 10;


  static {
    String version = "2.0.0.0";
    try (InputStream resourceAsStream = DriverInfo.class.getClassLoader().getResourceAsStream("version.properties")) {
      Properties versionFromBuild = new Properties();
      versionFromBuild.load(resourceAsStream);
      version = versionFromBuild.getProperty("version");
    } catch (IOException ex) {
      // do nothing
    }
    String[] versionComponents = version.split(".");
    int majorVersion = 2;
    int minorVersion = 0;
    int patchVersion = 0;
    try {
      if (versionComponents.length >= 3) {
        majorVersion = Integer.parseInt(versionComponents[0]);
        minorVersion = Integer.parseInt(versionComponents[1]);
        patchVersion = Integer.parseInt(versionComponents[2]);
      }
    } catch (NumberFormatException ex) {
      majorVersion = 2;
      minorVersion = 0;
      patchVersion = 0;
    }
    MAJOR_VERSION = majorVersion;
    MINOR_VERSION = minorVersion;
    PATCH_VERSION = patchVersion;
    DRIVER_VERSION = version;
    DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;
  }
  private DriverInfo() {
  }

}
