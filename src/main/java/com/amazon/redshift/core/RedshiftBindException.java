/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import java.io.IOException;

public class RedshiftBindException extends IOException {

  private final IOException ioe;

  public RedshiftBindException(IOException ioe) {
    this.ioe = ioe;
  }

  public IOException getIOException() {
    return ioe;
  }
}
