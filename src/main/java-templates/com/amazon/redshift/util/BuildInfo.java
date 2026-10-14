/*
 * Copyright (c) Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.util;

/**
 * Build-time constants baked into the compiled class file.
 *
 * <p>This file is a template: the version token below is substituted with the
 * project version by the build (Maven templating-maven-plugin and the Gradle
 * generateBuildInfo task) before compilation.
 */
final class BuildInfo {

  static final String DRIVER_VERSION = "${project.version}";

  private BuildInfo() {
  }

}
