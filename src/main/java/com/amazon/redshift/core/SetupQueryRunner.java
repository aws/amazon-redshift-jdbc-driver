/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core;

import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Properties;

/**
 * Poor man's Statement &amp; ResultSet, used for initial queries while we're still initializing the
 * system.
 */
public class SetupQueryRunner {

  private static class SimpleResultHandler extends ResultHandlerBase {
    private List<Tuple> tuples;

    SimpleResultHandler() {
      // This class overrided the handleWarning method and ignore warnings.
      // No need to handle property value
      super(new Properties());
    }

    List<Tuple> getResults() {
      return tuples;
    }

    public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
        ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
        int[] rowCount, Thread ringBufferThread) {
      this.tuples = tuples;
    }

    public void handleWarning(SQLWarning warning) {
      // We ignore warnings. We assume we know what we're
      // doing in the setup queries.
    }
  }

  public static Tuple run(QueryExecutor executor, String queryString,
      boolean wantResults) throws SQLException {
    Query query = executor.createSimpleQuery(queryString);
    SimpleResultHandler handler = new SimpleResultHandler();

    int flags = QueryExecutor.QUERY_ONESHOT | QueryExecutor.QUERY_SUPPRESS_BEGIN
        | QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    if (!wantResults) {
      flags |= QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_NO_METADATA;
    }

    try {
      executor.execute(query, null, handler, 0, 0, flags);
    } finally {
      query.close();
    }

    if (!wantResults) {
      return null;
    }

    List<Tuple> tuples = handler.getResults();
    if (tuples == null || tuples.size() != 1) {
      throw new RedshiftException(GT.tr("An unexpected result was returned by a query."),
          RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
    }

    return tuples.get(0);
  }

}
