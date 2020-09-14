/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.ParameterList;
import com.amazon.redshift.core.Query;
import com.amazon.redshift.core.ResultCursor;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;

import java.util.List;

class CallableBatchResultHandler extends BatchResultHandler {
  CallableBatchResultHandler(RedshiftStatementImpl statement, Query[] queries, ParameterList[] parameterLists) {
    super(statement, queries, parameterLists, false);
  }

  public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples, ResultCursor cursor, 
  							RedshiftRowsBlockingQueue<Tuple> queueTuples, int[] rowCount, Thread ringBufferThread) {
    /* ignore */
  }
}
