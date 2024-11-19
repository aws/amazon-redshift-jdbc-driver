/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.ParameterList;
import com.amazon.redshift.core.Query;
import com.amazon.redshift.core.ResultCursor;
import com.amazon.redshift.core.ResultHandlerBase;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.v3.BatchedQuery;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Internal class, it is not a part of public API.
 */
public class BatchResultHandler extends ResultHandlerBase {

  private final RedshiftStatementImpl rsStatement;
  private int resultIndex = 0;

  private final Query[] queries;
  private final long[] longUpdateCounts;
  private final ParameterList[] parameterLists;
  private final boolean expectGeneratedKeys;
  private RedshiftResultSet generatedKeys;
  private int committedRows; // 0 means no rows committed. 1 means row 0 was committed, and so on
  private final List<List<Tuple>> allGeneratedRows;
  private List<Tuple> latestGeneratedRows;
  private RedshiftResultSet latestGeneratedKeysRs;

  BatchResultHandler(RedshiftStatementImpl rsStatement, Query[] queries, ParameterList[] parameterLists,
      boolean expectGeneratedKeys) {
    super(rsStatement.getConnectionProperties());
    this.rsStatement = rsStatement;
    this.queries = queries;
    this.parameterLists = parameterLists;
    this.longUpdateCounts = new long[queries.length];
    this.expectGeneratedKeys = expectGeneratedKeys;
    this.allGeneratedRows = !expectGeneratedKeys ? null : new ArrayList<List<Tuple>>();
  }

  @Override
  public void setStatementStateIdleFromInQuery() {
  	rsStatement.updateStatementCancleState(StatementCancelState.IN_QUERY, StatementCancelState.IDLE);
  }

  @Override
  public void setStatementStateInQueryFromIdle() {
  	rsStatement.updateStatementCancleState(StatementCancelState.IDLE, StatementCancelState.IN_QUERY);
  }
  
  @Override
  public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
      ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
      	 int[] rowCount, Thread ringBufferThread) {
    // If SELECT, then handleCommandStatus call would just be missing
    resultIndex++;
    if (!expectGeneratedKeys) {
      // No rows expected -> just ignore rows
      return;
    }
    if (generatedKeys == null) {
      try {
        // If SELECT, the resulting ResultSet is not valid
        // Thus it is up to handleCommandStatus to decide if resultSet is good enough
        latestGeneratedKeysRs = (RedshiftResultSet) rsStatement.createResultSet(fromQuery, fields,
            new ArrayList<Tuple>(), cursor, queueTuples, rowCount, ringBufferThread);
      } catch (SQLException e) {
        handleError(e);
      }
    }
    latestGeneratedRows = tuples;
  }

  @Override
  public void handleCommandStatus(String status, long updateCount, long insertOID) {
    if (latestGeneratedRows != null) {
      // We have DML. Decrease resultIndex that was just increased in handleResultRows
      resultIndex--;
      // If exception thrown, no need to collect generated keys
      // Note: some generated keys might be secured in generatedKeys
      if (updateCount > 0 && (getException() == null || isAutoCommit())) {
        allGeneratedRows.add(latestGeneratedRows);
        if (generatedKeys == null) {
          generatedKeys = latestGeneratedKeysRs;
        }
      }
      latestGeneratedRows = null;
    }

    if (resultIndex >= queries.length) {
      handleError(new RedshiftException(GT.tr("Too many update results were returned."),
          RedshiftState.TOO_MANY_RESULTS));
      return;
    }
    latestGeneratedKeysRs = null;

    longUpdateCounts[resultIndex++] = updateCount;
  }

  private boolean isAutoCommit() {
    try {
      return rsStatement.getConnection().getAutoCommit();
    } catch (SQLException e) {
      assert false : "pgStatement.getConnection().getAutoCommit() should not throw";
      return false;
    }
  }

  @Override
  public void secureProgress() {
    if (isAutoCommit()) {
      committedRows = resultIndex;
      updateGeneratedKeys();
    }
  }

  private void updateGeneratedKeys() {
    if (allGeneratedRows == null || allGeneratedRows.isEmpty()) {
      return;
    }
    for (List<Tuple> rows : allGeneratedRows) {
      generatedKeys.addRows(rows);
    }
    allGeneratedRows.clear();
  }

  @Override
  public void handleWarning(SQLWarning warning) {
    rsStatement.addWarning(warning);
  }

  @Override
  public void handleError(SQLException newError) {
    if (getException() == null) {
      Arrays.fill(longUpdateCounts, committedRows, longUpdateCounts.length, Statement.EXECUTE_FAILED);
      if (allGeneratedRows != null) {
        allGeneratedRows.clear();
      }

      String queryString = "<unknown>";
      if (resultIndex < queries.length) {
        queryString = queries[resultIndex].toString(parameterLists[resultIndex]);
      }

      BatchUpdateException batchException;
      //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
      batchException = new BatchUpdateException(
          GT.tr("Batch entry {0} {1} was aborted: {2}  Call getNextException to see other errors in the batch.",
              resultIndex, queryString, newError.getMessage()),
          newError.getSQLState(), 0, uncompressLongUpdateCount(), newError);
      //JCP! else
//JCP>       batchException = new BatchUpdateException(
//JCP>           GT.tr("Batch entry {0} {1} was aborted: {2}  Call getNextException to see other errors in the batch.",
//JCP>               resultIndex, queryString, newError.getMessage()),
//JCP>           newError.getSQLState(), 0, uncompressUpdateCount(), newError);
      //JCP! endif

      super.handleError(batchException);
    }
    resultIndex++;

    super.handleError(newError);
  }

  @Override
  public void handleCompletion() throws SQLException {
    updateGeneratedKeys();
    SQLException batchException = getException();
    if (batchException != null) {
      if (isAutoCommit()) {
        // Re-create batch exception since rows after exception might indeed succeed.
        BatchUpdateException newException;
        //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
        newException = new BatchUpdateException(
            batchException.getMessage(),
            batchException.getSQLState(), 0,
            uncompressLongUpdateCount(),
            batchException.getCause()
        );
        //JCP! else
//JCP>         newException = new BatchUpdateException(
//JCP>             batchException.getMessage(),
//JCP>             batchException.getSQLState(), 0,
//JCP>             uncompressUpdateCount(),
//JCP>             batchException.getCause()
//JCP>         );
        //JCP! endif

        SQLException next = batchException.getNextException();
        if (next != null) {
          newException.setNextException(next);
        }
        batchException = newException;
      }
      throw batchException;
    }
  }

  public ResultSet getGeneratedKeys() {
    return generatedKeys;
  }

  private int[] uncompressUpdateCount() {
    long[] original = uncompressLongUpdateCount();
    int[] copy = new int[original.length];
    for (int i = 0; i < original.length; i++) {
      copy[i] = original[i] > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : (int) original[i];
    }
    return copy;
  }

  public int[] getUpdateCount() {
    return uncompressUpdateCount();
  }

  private long[] uncompressLongUpdateCount() {
    if (!(queries[0] instanceof BatchedQuery)) {
      return longUpdateCounts;
    }
    int totalRows = 0;
    boolean hasRewrites = false;
    for (Query query : queries) {
      int batchSize = query.getBatchSize();
      totalRows += batchSize;
      hasRewrites |= batchSize > 1;
    }
    if (!hasRewrites) {
      return longUpdateCounts;
    }

    /* In this situation there is a batch that has been rewritten. Substitute
     * the running total returned by the database with a status code to
     * indicate successful completion for each row the driver client added
     * to the batch.
     */
    long[] newUpdateCounts = new long[totalRows];
    int offset = 0;
    for (int i = 0; i < queries.length; i++) {
      Query query = queries[i];
      int batchSize = query.getBatchSize();
      long superBatchResult = longUpdateCounts[i];
      if (batchSize == 1) {
        newUpdateCounts[offset++] = superBatchResult;
        continue;
      }
      if (superBatchResult > 0) {
        // If some rows inserted, we do not really know how did they spread over individual
        // statements
        superBatchResult = Statement.SUCCESS_NO_INFO;
      }
      Arrays.fill(newUpdateCounts, offset, offset + batchSize, superBatchResult);
      offset += batchSize;
    }
    return newUpdateCounts;
  }

  public long[] getLargeUpdateCount() {
    return uncompressLongUpdateCount();
  }

}
