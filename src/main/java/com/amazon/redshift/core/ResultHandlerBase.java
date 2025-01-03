/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Properties;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.jdbc.RedshiftWarningWrapper;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;

/**
 * Empty implementation of {@link ResultHandler} interface.
 * {@link SQLException#setNextException(SQLException)} has {@code O(N)} complexity,
 * so this class tracks the last exception object to speedup {@code setNextException}.
 */
public class ResultHandlerBase implements ResultHandler {
  // Last exception is tracked to avoid O(N) SQLException#setNextException just in case there
  // will be lots of exceptions (e.g. all batch rows fail with constraint violation or so)
  private SQLException firstException;
  private SQLException lastException;

  private RedshiftWarningWrapper warningChain;
  Properties props;

  public ResultHandlerBase(Properties inProps) {
    this.props = inProps;
  }

  @Override
  public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
      ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
      int[] rowCount, Thread ringBufferThread) {
  }

  @Override
  public void handleCommandStatus(String status, long updateCount, long insertOID) {
  }

  @Override
  public void secureProgress() {
  }

  @Override
  public void handleWarning(SQLWarning warning) {
    if (warningChain == null) {
      warningChain = new RedshiftWarningWrapper(warning, props);
    } else {
      warningChain.appendWarning(warning);
    }
  }

  @Override
  public void handleError(SQLException error) {
    if (firstException == null) {
      firstException = lastException = error;
      return;
    }
    lastException.setNextException(error);
    lastException = error;
  }

  @Override
  public void handleCompletion() throws SQLException {
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public SQLException getException() {
    return firstException;
  }

  @Override
  public SQLWarning getWarning() {
    if (warningChain == null) {
      return null;
    }
    return warningChain.getFirstWarning();
  }
  
  @Override
  public void setStatementStateIdleFromInQuery() {
  	// Do nothing
  }
  
  @Override
  public void setStatementStateInQueryFromIdle() {
  	// Do nothing
  }
  
  @Override
  public boolean wantsScrollableResultSet() {
  	return false;
  }
}
