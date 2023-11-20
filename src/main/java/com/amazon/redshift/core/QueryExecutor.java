/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core;

import com.amazon.redshift.RedshiftNotification;
import com.amazon.redshift.copy.CopyOperation;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.core.v3.TypeTransferModeRegistry;
import com.amazon.redshift.jdbc.AutoSave;
import com.amazon.redshift.jdbc.BatchResultHandler;
import com.amazon.redshift.jdbc.EscapeSyntaxCallMode;
import com.amazon.redshift.jdbc.PreferQueryMode;
import com.amazon.redshift.util.HostSpec;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * <p>Abstracts the protocol-specific details of executing a query.</p>
 *
 * <p>Every connection has a single QueryExecutor implementation associated with it. This object
 * provides:</p>
 *
 * <ul>
 * <li>factory methods for Query objects ({@link #createSimpleQuery(String)} and
 * {@link #createQuery(String, boolean, boolean, String...)})
 * <li>execution methods for created Query objects (
 * {@link #execute(Query, ParameterList, ResultHandler, int, int, int)} for single queries and
 * {@link #execute(Query[], ParameterList[], BatchResultHandler, int, int, int)} for batches of queries)
 * <li>a fastpath call interface ({@link #createFastpathParameters} and {@link #fastpathCall}).
 * </ul>
 *
 * <p>Query objects may represent a query that has parameter placeholders. To provide actual values for
 * these parameters, a {@link ParameterList} object is created via a factory method (
 * {@link Query#createParameterList}). The parameters are filled in by the caller and passed along
 * with the query to the query execution methods. Several ParameterLists for a given query might
 * exist at one time (or over time); this allows the underlying Query to be reused for several
 * executions, or for batch execution of the same Query.</p>
 *
 * <p>In general, a Query created by a particular QueryExecutor may only be executed by that
 * QueryExecutor, and a ParameterList created by a particular Query may only be used as parameters
 * to that Query. Unpredictable things will happen if this isn't done.</p>
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface QueryExecutor extends TypeTransferModeRegistry {
  /**
   * Flag for query execution that indicates the given Query object is unlikely to be reused.
   */
  int QUERY_ONESHOT = 1;

  /**
   * Flag for query execution that indicates that resultset metadata isn't needed and can be safely
   * omitted.
   */
  int QUERY_NO_METADATA = 2;

  /**
   * Flag for query execution that indicates that a resultset isn't expected and the query executor
   * can safely discard any rows (although the resultset should still appear to be from a
   * resultset-returning query).
   */
  int QUERY_NO_RESULTS = 4;

  /**
   * Flag for query execution that indicates a forward-fetch-capable cursor should be used if
   * possible.
   */
  int QUERY_FORWARD_CURSOR = 8;

  /**
   * Flag for query execution that indicates the automatic BEGIN on the first statement when outside
   * a transaction should not be done.
   */
  int QUERY_SUPPRESS_BEGIN = 16;

  /**
   * Flag for query execution when we don't really want to execute, we just want to get the
   * parameter metadata for the statement.
   */
  int QUERY_DESCRIBE_ONLY = 32;

  /**
   * Flag for query execution used by generated keys where we want to receive both the ResultSet and
   * associated update count from the command status.
   */
  int QUERY_BOTH_ROWS_AND_STATUS = 64;

  /**
   * Force this query to be described at each execution. This is done in pipelined batches where we
   * might need to detect mismatched result types.
   */
  int QUERY_FORCE_DESCRIBE_PORTAL = 512;

  /**
   * Flag to disable batch execution when we expect results (generated keys) from a statement.
   *
   * @deprecated in PgJDBC 9.4 as we now auto-size batches.
   */
  @Deprecated
  int QUERY_DISALLOW_BATCHING = 128;

  /**
   * Flag for query execution to avoid using binary transfer.
   */
  int QUERY_NO_BINARY_TRANSFER = 256;

  /**
   * Execute the query via simple 'Q' command (not parse, bind, exec, but simple execute).
   * This sends query text on each execution, however it supports sending multiple queries
   * separated with ';' as a single command.
   */
  int QUERY_EXECUTE_AS_SIMPLE = 1024;

  int MAX_SAVE_POINTS = 1000;

  /**
   * Flag indicating that when beginning a transaction, it should be read only.
   */
  int QUERY_READ_ONLY_HINT = 2048;

  /**
   * Execute a Query, passing results to a provided ResultHandler.
   *
   * @param query the query to execute; must be a query returned from calling
   *        {@link #wrap(List)} on this QueryExecutor object.
   * @param parameters the parameters for the query. Must be non-<code>null</code> if the query
   *        takes parameters. Must be a parameter object returned by
   *        {@link com.amazon.redshift.core.Query#createParameterList()}.
   * @param handler a ResultHandler responsible for handling results generated by this query
   * @param maxRows the maximum number of rows to retrieve
   * @param fetchSize if QUERY_FORWARD_CURSOR is set, the preferred number of rows to retrieve
   *        before suspending
   * @param flags a combination of QUERY_* flags indicating how to handle the query.
   * @throws SQLException if query execution fails
   */
  void execute(Query query, ParameterList parameters, ResultHandler handler, int maxRows,
      int fetchSize, int flags) throws SQLException;

  /**
   * Execute several Query, passing results to a provided ResultHandler.
   *
   * @param queries the queries to execute; each must be a query returned from calling
   *        {@link #wrap(List)} on this QueryExecutor object.
   * @param parameterLists the parameter lists for the queries. The parameter lists correspond 1:1
   *        to the queries passed in the <code>queries</code> array. Each must be non-
   *        <code>null</code> if the corresponding query takes parameters, and must be a parameter
   *        object returned by {@link com.amazon.redshift.core.Query#createParameterList()} created by
   *        the corresponding query.
   * @param handler a ResultHandler responsible for handling results generated by this query
   * @param maxRows the maximum number of rows to retrieve
   * @param fetchSize if QUERY_FORWARD_CURSOR is set, the preferred number of rows to retrieve
   *        before suspending
   * @param flags a combination of QUERY_* flags indicating how to handle the query.
   * @throws SQLException if query execution fails
   */
  void execute(Query[] queries, ParameterList[] parameterLists, BatchResultHandler handler, int maxRows,
      int fetchSize, int flags) throws SQLException;

  /**
   * Fetch additional rows from a cursor.
   *
   * @param cursor the cursor to fetch from
   * @param handler the handler to feed results to
   * @param fetchSize the preferred number of rows to retrieve before suspending
   * @param initRowCount the number of rows already fetched
   * @throws SQLException if query execution fails
   */
  void fetch(ResultCursor cursor, ResultHandler handler, int fetchSize, int initRowCount) throws SQLException;

  /**
   * Create an unparameterized Query object suitable for execution by this QueryExecutor. The
   * provided query string is not parsed for parameter placeholders ('?' characters), and the
   * {@link Query#createParameterList} of the returned object will always return an empty
   * ParameterList.
   *
   * @param sql the SQL for the query to create
   * @return a new Query object
   * @throws SQLException if something goes wrong
   */
  Query createSimpleQuery(String sql) throws SQLException;

  boolean isReWriteBatchedInsertsEnabled();

  CachedQuery createQuery(String sql, boolean escapeProcessing, boolean isParameterized,
      String... columnNames)
      throws SQLException;

  Object createQueryKey(String sql, boolean escapeProcessing, boolean isParameterized,
      String... columnNames);

  CachedQuery createQueryByKey(Object key) throws SQLException;

  CachedQuery borrowQueryByKey(Object key) throws SQLException;

  CachedQuery borrowQuery(String sql) throws SQLException;

  CachedQuery borrowCallableQuery(String sql) throws SQLException;

  CachedQuery borrowReturningQuery(String sql, String[] columnNames) throws SQLException;

  void releaseQuery(CachedQuery cachedQuery);

  /**
   * Wrap given native query into a ready for execution format.
   * @param queries list of queries in native to database syntax
   * @return query object ready for execution by this query executor
   */
  Query wrap(List<NativeQuery> queries);

  /**
   * Prior to attempting to retrieve notifications, we need to pull any recently received
   * notifications off of the network buffers. The notification retrieval in ProtocolConnection
   * cannot do this as it is prone to deadlock, so the higher level caller must be responsible which
   * requires exposing this method.
   *
   * @throws SQLException if and error occurs while fetching notifications
   */
  void processNotifies() throws SQLException;

  /**
   * Prior to attempting to retrieve notifications, we need to pull any recently received
   * notifications off of the network buffers. The notification retrieval in ProtocolConnection
   * cannot do this as it is prone to deadlock, so the higher level caller must be responsible which
   * requires exposing this method. This variant supports blocking for the given time in millis.
   *
   * @param timeoutMillis number of milliseconds to block for
   * @throws SQLException if and error occurs while fetching notifications
   */
  void processNotifies(int timeoutMillis) throws SQLException;

  //
  // Fastpath interface.
  //

  /**
   * Create a new ParameterList implementation suitable for invoking a fastpath function via
   * {@link #fastpathCall}.
   *
   * @param count the number of parameters the fastpath call will take
   * @return a ParameterList suitable for passing to {@link #fastpathCall}.
   * @deprecated This API is somewhat obsolete, as one may achieve similar performance
   *         and greater functionality by setting up a prepared statement to define
   *         the function call. Then, executing the statement with binary transmission of parameters
   *         and results substitutes for a fast-path function call.
   */
  @Deprecated
  ParameterList createFastpathParameters(int count);

  /**
   * Invoke a backend function via the fastpath interface.
   *
   * @param fnid the OID of the backend function to invoke
   * @param params a ParameterList returned from {@link #createFastpathParameters} containing the
   *        parameters to pass to the backend function
   * @param suppressBegin if begin should be suppressed
   * @return the binary-format result of the fastpath call, or <code>null</code> if a void result
   *         was returned
   * @throws SQLException if an error occurs while executing the fastpath call
   * @deprecated This API is somewhat obsolete, as one may achieve similar performance
   *         and greater functionality by setting up a prepared statement to define
   *         the function call. Then, executing the statement with binary transmission of parameters
   *         and results substitutes for a fast-path function call.
   */
  @Deprecated
  byte[] fastpathCall(int fnid, ParameterList params, boolean suppressBegin) throws SQLException;

  /**
   * Issues a COPY FROM STDIN / COPY TO STDOUT statement and returns handler for associated
   * operation. Until the copy operation completes, no other database operation may be performed.
   * Implemented for protocol version 3 only.
   *
   * @param sql input sql
   * @param suppressBegin if begin should be suppressed
   * @return handler for associated operation
   * @throws SQLException when initializing the given query fails
   */
  CopyOperation startCopy(String sql, boolean suppressBegin) throws SQLException;

  /**
   * @return the version of the implementation
   */
  int getProtocolVersion();

  /**
   * Sets the oids that should be received using binary encoding.
   *
   * @param useBinaryForOids The oids to request with binary encoding.
   */
  void setBinaryReceiveOids(Set<Integer> useBinaryForOids);

  /**
   * Sets the oids that should be sent using binary encoding.
   *
   * @param useBinaryForOids The oids to send with binary encoding.
   */
  void setBinarySendOids(Set<Integer> useBinaryForOids);

  /**
   * Returns true if server uses integer instead of double for binary date and time encodings.
   *
   * @return the server integer_datetime setting.
   */
  boolean getIntegerDateTimes();

  /**
   * @return the host and port this connection is connected to.
   */
  HostSpec getHostSpec();

  /**
   * @return the user this connection authenticated as.
   */
  String getUser();

  /**
   * @return the database this connection is connected to.
   */
  String getDatabase();

  /**
   * Sends a query cancellation for this connection.
   *
   * @throws SQLException if something goes wrong.
   */
  void sendQueryCancel() throws SQLException;

  /**
   * Return the process ID (PID) of the backend server process handling this connection.
   *
   * @return process ID (PID) of the backend server process handling this connection
   */
  int getBackendPID();

  /**
   * Abort at network level without sending the Terminate message to the backend.
   */
  void abort();

  /**
   * Close this connection cleanly.
   */
  void close();

  /**
   * Check if this connection is closed.
   *
   * @return true iff the connection is closed.
   */
  boolean isClosed();

  /**
   * <p>Return the server version from the server_version GUC.</p>
   *
   * <p>Note that there's no requirement for this to be numeric or of the form x.y.z. Redshift
   * development releases usually have the format x.ydevel e.g. 9.4devel; betas usually x.ybetan
   * e.g. 9.4beta1. The --with-extra-version configure option may add an arbitrary string to this.</p>
   *
   * <p>Don't use this string for logic, only use it when displaying the server version to the user.
   * Prefer getServerVersionNum() for all logic purposes.</p>
   *
   * @return the server version string from the server_version guc
   */
  String getServerVersion();

  /**
   * Redshift supports different versions to accomodate
   * new request for optimization for better performance.
   * 
   * 0 means base protocol version
   * 1 means extended resultset metadata protocol version
   * 
   * @return the server protocol version.
   */
  int getServerProtocolVersion();

  /**
   * Get the server capability for datashare database query support.
   * This should enable for data sharing (either producer/consumer).
   *  
   * @return true means datashare database query is supported otherwise false.
   */
  boolean isDatashareEnabled();
  
  /**
   * Retrieve and clear the set of asynchronous notifications pending on this connection.
   *
   * @return an array of notifications; if there are no notifications, an empty array is returned.
   * @throws SQLException if and error occurs while fetching notifications
   */
  RedshiftNotification[] getNotifications() throws SQLException;

  /**
   * Retrieve and clear the chain of warnings accumulated on this connection.
   *
   * @return the first SQLWarning in the chain; subsequent warnings can be found via
   *         SQLWarning.getNextWarning().
   */
  SQLWarning getWarnings();

  /**
   * <p>Get a machine-readable server version.</p>
   *
   * <p>This returns the value of the server_version_num GUC. If no such GUC exists, it falls back on
   * attempting to parse the text server version for the major version. If there's no minor version
   * (e.g. a devel or beta release) then the minor version is set to zero. If the version could not
   * be parsed, zero is returned.</p>
   *
   * @return the server version in numeric XXYYZZ form, eg 090401, from server_version_num
   */
  int getServerVersionNum();

  /**
   * Get the current transaction state of this connection.
   *
   * @return a ProtocolConnection.TRANSACTION_* constant.
   */
  TransactionState getTransactionState();

  /**
   * Returns whether the server treats string-literals according to the SQL standard or if it uses
   * traditional Redshift escaping rules. Versions up to 8.1 always treated backslashes as escape
   * characters in string-literals. Since 8.2, this depends on the value of the
   * {@code standard_conforming_strings} server variable.
   *
   * @return true if the server treats string literals according to the SQL standard
   */
  boolean getStandardConformingStrings();

  /**
   * Returns backend timezone in java format.
   * @return backend timezone in java format.
   */
  TimeZone getTimeZone();

  /**
   * @return the current encoding in use by this connection
   */
  Encoding getEncoding();

  /**
   * Returns application_name connection property.
   * @return application_name connection property
   */
  String getApplicationName();

  boolean isColumnSanitiserDisabled();

  EscapeSyntaxCallMode getEscapeSyntaxCallMode();

  PreferQueryMode getPreferQueryMode();

  AutoSave getAutoSave();

  void setAutoSave(AutoSave autoSave);

  boolean willHealOnRetry(SQLException e);

  boolean isRaiseExceptionOnSilentRollback();

  void setRaiseExceptionOnSilentRollback(boolean raiseExceptionOnSilentRollback);

  /**
   * By default, the connection resets statement cache in case deallocate all/discard all
   * message is observed.
   * This API allows to disable that feature for testing purposes.
   *
   * @param flushCacheOnDeallocate true if statement cache should be reset when "deallocate/discard" message observed
   */
  void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate);

  /**
   * @return the ReplicationProtocol instance for this connection.
   */
  ReplicationProtocol getReplicationProtocol();

  void setNetworkTimeout(int milliseconds) throws IOException;

  int getNetworkTimeout() throws IOException;

  // Expose parameter status to RedshiftConnection
  Map<String,String> getParameterStatuses();

  String getParameterStatus(String parameterName);
  
  /**
   * Close the last active ring buffer thread.
   * 
   * @param queueRows the blocking queue rows
   * @param ringBufferThread the thread fetching rows in the blocking queue.
   */
  void closeRingBufferThread(RedshiftRowsBlockingQueue<Tuple> queueRows, Thread ringBufferThread);
  
  /**
   * Check for a running ring buffer thread. 
   * 
   * @return returns true if Ring buffer thread is running, otherwise false.
   */
  boolean isRingBufferThreadRunning();

  void waitForRingBufferThreadToFinish(boolean calledFromConnectionClose,
                                       boolean calledFromResultsetClose,
                                       boolean calledFromStatementClose,
                                       RedshiftRowsBlockingQueue<Tuple> queueRows,
                                       Thread ringBufferThread);
  
  /**
   * Close the statement and portal when statement get closed.
   */
  void closeStatementAndPortal();  
  
  /**
   * Get multiple SQL supports.
   * 
   * @return true, if supported. false otherwise.
   */
  boolean isMultiSqlSupport();

  /**
   * Get the server capability for cross-datashare database query support.
   * This should enable for cross data sharing.
   *
   * @return true means cross-datashare database query is supported otherwise false.
   */
  boolean isCrossDatasharingEnabled();
}
