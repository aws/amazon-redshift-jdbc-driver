/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.xa;

import javax.transaction.xa.XAException;

/**
 * A convenience subclass of <code>XAException</code> which makes it easy to create an instance of
 * <code>XAException</code> with a human-readable message, a <code>Throwable</code> cause, and an XA
 * error code.
 *
 * @author Michael S. Allman
 */
public class RedshiftXAException extends XAException {
  RedshiftXAException(String message, int errorCode) {
    super(message);

    this.errorCode = errorCode;
  }

  RedshiftXAException(String message, Throwable cause, int errorCode) {
    super(message);

    initCause(cause);
    this.errorCode = errorCode;
  }

  RedshiftXAException(Throwable cause, int errorCode) {
    super(errorCode);

    initCause(cause);
  }
}
