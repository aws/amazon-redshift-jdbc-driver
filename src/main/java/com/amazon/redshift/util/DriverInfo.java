/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

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
    // BuildInfo.DRIVER_VERSION is a compile-time constant generated from the
    // project version by the build (see the generateBuildInfo Gradle task and
    // the templating-maven-plugin execution in the POM).
    DRIVER_VERSION = BuildInfo.DRIVER_VERSION;
    int[] components = parseVersionComponents(DRIVER_VERSION);
    MAJOR_VERSION = components[0];
    MINOR_VERSION = components[1];
    PATCH_VERSION = components[2];
    DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;
  }

  /**
   * Parses the major, minor and patch numbers from a version string of the form
   * {@code X.Y.Z} or {@code X.Y.Z.W}. Falls back to {@code {2, 0, 0}} when the
   * version is null, has fewer than three components, or has a non-numeric
   * major/minor/patch component.
   *
   * @param version the version string to parse (may be {@code null})
   * @return a three-element array holding {major, minor, patch}
   */
  static int[] parseVersionComponents(String version) {
    int majorVersion = 2;
    int minorVersion = 0;
    int patchVersion = 0;
    String[] versionComponents = version == null ? new String[0] : version.split("\\.");
    if (versionComponents.length >= 3) {
      try {
        majorVersion = Integer.parseInt(versionComponents[0]);
        minorVersion = Integer.parseInt(versionComponents[1]);
        patchVersion = Integer.parseInt(versionComponents[2]);
      } catch (NumberFormatException ex) {
        majorVersion = 2;
        minorVersion = 0;
        patchVersion = 0;
      }
    }
    return new int[] {majorVersion, minorVersion, patchVersion};
  }

  private DriverInfo() {
  }

}
