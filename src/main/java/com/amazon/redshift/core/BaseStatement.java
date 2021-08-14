/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.RedshiftStatement;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftByteBufferBlockingQueue;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Driver-internal statement interface. Application code should not use this interface.
 */
public interface BaseStatement extends RedshiftStatement, Statement {
  /**
   * Create a synthetic resultset from data provided by the driver.
   *
   * @param fields the column metadata for the resultset
   * @param tuples the resultset data
   * @return the new ResultSet
   * @throws SQLException if something goes wrong
   */
  ResultSet createDriverResultSet(Field[] fields, List<Tuple> tuples) throws SQLException;

  /**
   * Create a resultset from data retrieved from the server.
   *
   * @param originalQuery the query that generated this resultset; used when dealing with updateable
   *        resultsets
   * @param fields the column metadata for the resultset
   * @param tuples the resultset data
   * @param cursor the cursor to use to retrieve more data from the server; if null, no additional
   *        data is present.
   * @return the new ResultSet
   * @throws SQLException if something goes wrong
   */
  
  /**
   * 
   * Create a resultset from data retrieved from the server.
   *
   * @param originalQuery the query that generated this resultset; used when dealing with updateable
   *        resultsets
   * @param fields the column metadata for the resultset
   * @param tuples the offsets to the resultset data
   * @param cursor the cursor to use to retrieve more data from the server; if null, no additional
   *        data is present.
   * @param queueTuples the offsets to the actual data in a blocking queue. If this is set then tuples will be null.
   * @param queuePages the pages in memory that are available to be read from
	 * @param rowCount number of rows fetched from the socket.
	 * @param ringBufferThread a thread to fetch data in the buffer.
   * @param processBufferThread a thread to process data from the buffer
   * 
   * @return the new ResultSet
   * @throws SQLException if something goes wrong
   */
  ResultSet createResultSet(Query originalQuery, Field[] fields, List<Tuple> tuples,
      ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples, RedshiftByteBufferBlockingQueue<ByteBuffer> queuePages,
      int[] rowCount, Thread ringBufferThread, Thread processBufferThread) throws SQLException;

  /**
   * Execute a query, passing additional query flags.
   *
   * @param sql the query to execute (JDBC-style query)
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(String sql, int flags) throws SQLException;

  /**
   * Execute a query, passing additional query flags.
   *
   * @param cachedQuery the query to execute (native to Redshift)
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(CachedQuery cachedQuery, int flags) throws SQLException;

  /**
   * Execute a prepared query, passing additional query flags.
   *
   * @param flags additional {@link QueryExecutor} flags for execution; these are bitwise-ORed into
   *        the default flags.
   * @return true if there is a result set
   * @throws SQLException if something goes wrong.
   */
  boolean executeWithFlags(int flags) throws SQLException;
}
