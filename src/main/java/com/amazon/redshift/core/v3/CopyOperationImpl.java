/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core.v3;

import java.sql.SQLException;

import com.amazon.redshift.copy.CopyOperation;
import com.amazon.redshift.jdbc.ResourceLock;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

public abstract class CopyOperationImpl implements CopyOperation {
  QueryExecutorImpl queryExecutor;
  int rowFormat;
  int[] fieldFormats;
  long handledRowCount = -1;
  private ResourceLock lock = new ResourceLock();

  void init(QueryExecutorImpl q, int fmt, int[] fmts) {
    queryExecutor = q;
    rowFormat = fmt;
    fieldFormats = fmts;
  }

  public void cancelCopy() throws SQLException {
    queryExecutor.cancelCopy(this);
  }

  public int getFieldCount() {
    return fieldFormats.length;
  }

  public int getFieldFormat(int field) {
    return fieldFormats[field];
  }

  public int getFormat() {
    return rowFormat;
  }

  public boolean isActive() {
	try (ResourceLock ignore = lock.obtain()) {
      return queryExecutor.hasLock(this);
    }
  }

  public void handleCommandStatus(String status) throws RedshiftException {
    if (status.startsWith("COPY")) {
      int i = status.lastIndexOf(' ');
      handledRowCount = i > 3 ? Long.parseLong(status.substring(i + 1)) : -1;
    } else {
      throw new RedshiftException(GT.tr("CommandComplete expected COPY but got: " + status),
          RedshiftState.COMMUNICATION_ERROR);
    }
  }

  /**
   * Consume received copy data.
   *
   * @param data data that was receive by copy protocol
   * @throws RedshiftException if some internal problem occurs
   */
  protected abstract void handleCopydata(byte[] data) throws RedshiftException;

  public long getHandledRowCount() {
    return handledRowCount;
  }
}
