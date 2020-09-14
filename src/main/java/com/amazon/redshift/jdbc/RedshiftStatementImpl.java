/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.Driver;
import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.core.BaseStatement;
import com.amazon.redshift.core.CachedQuery;
import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.ParameterList;
import com.amazon.redshift.core.Query;
import com.amazon.redshift.core.QueryExecutor;
import com.amazon.redshift.core.ResultCursor;
import com.amazon.redshift.core.ResultHandlerBase;
import com.amazon.redshift.core.SqlCommand;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class RedshiftStatementImpl implements Statement, BaseStatement {
  private static final String[] NO_RETURNING_COLUMNS = new String[0];

  /**
   * Default state for use or not binary transfers. Can use only for testing purposes
   */
  private static final boolean DEFAULT_FORCE_BINARY_TRANSFERS =
      Boolean.getBoolean("com.amazon.redshift.forceBinary");
  // only for testing purposes. even single shot statements will use binary transfers
  private boolean forceBinaryTransfers = DEFAULT_FORCE_BINARY_TRANSFERS;

  protected ArrayList<Query> batchStatements = null;
  protected ArrayList<ParameterList> batchParameters = null;
  protected final int resultsettype; // the resultset type to return (ResultSet.TYPE_xxx)
  protected final int concurrency; // is it updateable or not? (ResultSet.CONCUR_xxx)
  private final int rsHoldability;
  private boolean poolable;
  private boolean closeOnCompletion = false;
  protected int fetchdirection = ResultSet.FETCH_FORWARD;
  // fetch direction hint (currently ignored)

  /**
   * Protects current statement from cancelTask starting, waiting for a bit, and waking up exactly
   * on subsequent query execution. The idea is to atomically compare and swap the reference to the
   * task, so the task can detect that statement executes different query than the one the
   * cancelTask was created. Note: the field must be set/get/compareAndSet via
   * {@link #CANCEL_TIMER_UPDATER} as per {@link AtomicReferenceFieldUpdater} javadoc.
   */
  private volatile TimerTask cancelTimerTask = null;
  private static final AtomicReferenceFieldUpdater<RedshiftStatementImpl, TimerTask> CANCEL_TIMER_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(RedshiftStatementImpl.class, TimerTask.class, "cancelTimerTask");

  /**
   * Protects statement from out-of-order cancels. It protects from both
   * {@link #setQueryTimeout(int)} and {@link #cancel()} induced ones.
   *
   * {@link #execute(String)} and friends change the field to
   * {@link StatementCancelState#IN_QUERY} during execute. {@link #cancel()}
   * ignores cancel request if state is {@link StatementCancelState#IDLE}.
   * In case {@link #execute(String)} observes non-{@link StatementCancelState#IDLE} state as it
   * completes the query, it waits till {@link StatementCancelState#CANCELLED}. Note: the field must be
   * set/get/compareAndSet via {@link #STATE_UPDATER} as per {@link AtomicIntegerFieldUpdater}
   * javadoc.
   */
  private volatile StatementCancelState statementState = StatementCancelState.IDLE;

  private static final AtomicReferenceFieldUpdater<RedshiftStatementImpl, StatementCancelState> STATE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(RedshiftStatementImpl.class, StatementCancelState.class, "statementState");

  /**
   * Does the caller of execute/executeUpdate want generated keys for this execution? This is set by
   * Statement methods that have generated keys arguments and cleared after execution is complete.
   */
  protected boolean wantsGeneratedKeysOnce = false;

  /**
   * Was this PreparedStatement created to return generated keys for every execution? This is set at
   * creation time and never cleared by execution.
   */
  public boolean wantsGeneratedKeysAlways = false;

  // The connection who created us
  protected final BaseConnection connection;

  /**
   * The warnings chain.
   */
  protected volatile RedshiftWarningWrapper warnings = null;

  /**
   * Maximum number of rows to return, 0 = unlimited.
   */
  protected int maxrows = 0;

  /**
   * Number of rows to get in a batch.
   */
  protected int fetchSize = 0;

  /**
   * Timeout (in milliseconds) for a query.
   */
  protected long timeout = 0;

  protected boolean replaceProcessingEnabled = true;

  /**
   * The current results.
   */
  protected ResultWrapper result = null;

  /**
   * The first unclosed result.
   */
  protected volatile ResultWrapper firstUnclosedResult = null;

  /**
   * Results returned by a statement that wants generated keys.
   */
  protected ResultWrapper generatedKeys = null;

  protected int mPrepareThreshold; // Reuse threshold to enable use of PREPARE

  protected int maxFieldSize = 0;

  RedshiftStatementImpl(RedshiftConnectionImpl c, int rsType, int rsConcurrency, int rsHoldability)
      throws SQLException {
    this.connection = c;
    forceBinaryTransfers |= c.getForceBinary();
    resultsettype = rsType;
    concurrency = rsConcurrency;
    setFetchSize(c.getDefaultFetchSize());
    setPrepareThreshold(c.getPrepareThreshold());
    this.rsHoldability = rsHoldability;
  }

  public ResultSet createResultSet(Query originalQuery, Field[] fields, List<Tuple> tuples,
      ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
      int[] rowCount, Thread ringBufferThread) throws SQLException {
    RedshiftResultSet newResult = new RedshiftResultSet(originalQuery, this, fields, tuples, cursor,
        getMaxRows(), getMaxFieldSize(), getResultSetType(), getResultSetConcurrency(),
        getResultSetHoldability(), queueTuples, rowCount, ringBufferThread);
    newResult.setFetchSize(getFetchSize());
    newResult.setFetchDirection(getFetchDirection());
    return newResult;
  }

  public BaseConnection getRedshiftConnection() {
    return connection;
  }

  public String getFetchingCursorName() {
    return null;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  protected boolean wantsScrollableResultSet() {
    return resultsettype != ResultSet.TYPE_FORWARD_ONLY;
  }

  protected boolean wantsHoldableResultSet() {
    // FIXME: false if not supported
    return rsHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }
  
  /**
   * Internal use only.
   * 
   * @param oldState old state of the statement
   * @param newState new state of the statement
   */
  public void updateStatementCancleState(StatementCancelState oldState, StatementCancelState newState) {
  	STATE_UPDATER.compareAndSet(this, oldState, newState);
  }

  /**
   * ResultHandler implementations for updates, queries, and either-or.
   */
  public class StatementResultHandler extends ResultHandlerBase {
    private ResultWrapper results;
    private ResultWrapper lastResult;
    private Statement stmt;
    
    public StatementResultHandler(Statement stmt) {
    	this.stmt = stmt;
    }
    
    @Override
    public void setStatementStateIdleFromInQuery() {
    	((RedshiftStatementImpl)stmt).updateStatementCancleState(StatementCancelState.IN_QUERY, StatementCancelState.IDLE);
    }

    @Override
    public void setStatementStateInQueryFromIdle() {
    	((RedshiftStatementImpl)stmt).updateStatementCancleState(StatementCancelState.IDLE, StatementCancelState.IN_QUERY);
    }
    
    @Override
    public boolean wantsScrollableResultSet() {
    	return RedshiftStatementImpl.this.wantsScrollableResultSet();
    }
    
    ResultWrapper getResults() {
      return results;
    }

    private void append(ResultWrapper newResult) {
      if (results == null) {
        lastResult = results = newResult;
      } else {
        lastResult.append(newResult);
      }
    }

    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
        ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
        int[] rowCount, Thread ringBufferThread) {
      try {
        ResultSet rs = RedshiftStatementImpl.this.createResultSet(fromQuery, fields, tuples, cursor, 
        												queueTuples, rowCount, ringBufferThread);
        append(new ResultWrapper(rs));
      } catch (SQLException e) {
        handleError(e);
      }
    }

    @Override
    public void handleCommandStatus(String status, long updateCount, long insertOID) {
      append(new ResultWrapper(updateCount, insertOID));
    }

    @Override
    public void handleWarning(SQLWarning warning) {
      RedshiftStatementImpl.this.addWarning(warning);
    }

  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql);
  	
    if (!executeWithFlags(sql, 0)) {
      throw new RedshiftException(GT.tr("No results were returned by the query."), RedshiftState.NO_DATA);
    }

    ResultSet rs = getSingleResultSet();
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rs);
    
    return rs;
  }

  protected ResultSet getSingleResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result.getNext() != null) {
        throw new RedshiftException(GT.tr("Multiple ResultSets were returned by the query."),
            RedshiftState.TOO_MANY_RESULTS);
      }

      return result.getResultSet();
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql);
    
    executeWithFlags(sql, QueryExecutor.QUERY_NO_RESULTS);
    checkNoResultUpdate();
    int rc = getUpdateCount();

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  protected final void checkNoResultUpdate() throws SQLException {
    synchronized (this) {
      checkClosed();
      ResultWrapper iter = result;
      while (iter != null) {
        if (iter.getResultSet() != null) {
          throw new RedshiftException(GT.tr("A result was returned when none was expected."),
              RedshiftState.TOO_MANY_RESULTS);
        }
        iter = iter.getNext();
      }
    }
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, sql);
  	
    boolean rc = executeWithFlags(sql, 0);

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(false);
    
    return rc;
  }

  @Override
  public boolean executeWithFlags(String sql, int flags) throws SQLException {
    return executeCachedSql(sql, flags, NO_RETURNING_COLUMNS);
  }

  private boolean executeCachedSql(String sql, int flags, String[] columnNames) throws SQLException {
    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    // Simple statements should not replace ?, ? with $1, $2
    boolean shouldUseParameterized = false;
    QueryExecutor queryExecutor = connection.getQueryExecutor();
    Object key = queryExecutor
        .createQueryKey(sql, replaceProcessingEnabled, shouldUseParameterized, columnNames);
    CachedQuery cachedQuery;
    boolean shouldCache = preferQueryMode == PreferQueryMode.EXTENDED_CACHE_EVERYTHING;
    if (shouldCache) {
      cachedQuery = queryExecutor.borrowQueryByKey(key);
    } else {
      cachedQuery = queryExecutor.createQueryByKey(key);
    }
    if (wantsGeneratedKeysOnce) {
      SqlCommand sqlCommand = cachedQuery.query.getSqlCommand();
      wantsGeneratedKeysOnce = sqlCommand != null && sqlCommand.isReturningKeywordPresent();
    }
    boolean res;
    try {
      res = executeWithFlags(cachedQuery, flags);
    } finally {
      if (shouldCache) {
        queryExecutor.releaseQuery(cachedQuery);
      }
    }
    return res;
  }

  public boolean executeWithFlags(CachedQuery simpleQuery, int flags) throws SQLException {
    checkClosed();
    if (connection.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) < 0) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }
    execute(simpleQuery, null, flags);
    synchronized (this) {
      checkClosed();
      return (result != null && result.getResultSet() != null);
    }
  }

  public boolean executeWithFlags(int flags) throws SQLException {
    checkClosed();
    throw new RedshiftException(GT.tr("Can''t use executeWithFlags(int) on a Statement."),
        RedshiftState.WRONG_OBJECT_TYPE);
  }

  protected void closeForNextExecution() throws SQLException {

    // Every statement execution clears any previous warnings.
    clearWarnings();

    // Close any existing resultsets associated with this statement.
    synchronized (this) {
      while (firstUnclosedResult != null) {
        RedshiftResultSet rs = (RedshiftResultSet)firstUnclosedResult.getResultSet();
        if (rs != null) {
          rs.closeInternally();
        }
        firstUnclosedResult = firstUnclosedResult.getNext();
      }
      result = null;

      if (generatedKeys != null) {
        if (generatedKeys.getResultSet() != null) {
          generatedKeys.getResultSet().close();
        }
        generatedKeys = null;
      }
    }
  }

  /**
   * Returns true if query is unlikely to be reused.
   *
   * @param cachedQuery to check (null if current query)
   * @return true if query is unlikely to be reused
   */
  protected boolean isOneShotQuery(CachedQuery cachedQuery) {
    if (cachedQuery == null) {
      return true;
    }
    cachedQuery.increaseExecuteCount();
    if ((mPrepareThreshold == 0 || cachedQuery.getExecuteCount() < mPrepareThreshold)
        && !getForceBinaryTransfer()) {
      return true;
    }
    return false;
  }

  protected final void execute(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    try {
      executeInternal(cachedQuery, queryParameters, flags);
    } catch (SQLException e) {
      // Don't retry composite queries as it might get partially executed
      if (cachedQuery.query.getSubqueries() != null
          || !connection.getQueryExecutor().willHealOnRetry(e)) {
        throw e;
      }
      cachedQuery.query.close();
      // Execute the query one more time
      executeInternal(cachedQuery, queryParameters, flags);
    }
  }

  private void executeInternal(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    closeForNextExecution();

    // Enable cursor-based resultset if possible.
    if (fetchSize > 0 && !wantsScrollableResultSet() && !connection.getAutoCommit()
        && !wantsHoldableResultSet()) {
      flags |= QueryExecutor.QUERY_FORWARD_CURSOR;
    }

    if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
      flags |= QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS;

      // If the no results flag is set (from executeUpdate)
      // clear it so we get the generated keys results.
      //
      if ((flags & QueryExecutor.QUERY_NO_RESULTS) != 0) {
        flags &= ~(QueryExecutor.QUERY_NO_RESULTS);
      }
    }

    if (isOneShotQuery(cachedQuery)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    }
    // Only use named statements after we hit the threshold. Note that only
    // named statements can be transferred in binary format.

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }
    if (connection.hintReadOnly()) {
      flags |= QueryExecutor.QUERY_READ_ONLY_HINT;
    }

    // updateable result sets do not yet support binary updates
    if (concurrency != ResultSet.CONCUR_READ_ONLY) {
      flags |= QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    }

    Query queryToExecute = cachedQuery.query;

    if (queryToExecute.isEmpty()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    if (!queryToExecute.isStatementDescribed() && forceBinaryTransfers
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Simple 'Q' execution does not need to know parameter types
      // When binaryTransfer is forced, then we need to know resulting parameter and column types,
      // thus sending a describe request.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler(this);
      connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler2, 0, 0,
          flags2);
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    StatementResultHandler handler = new StatementResultHandler(this);
    synchronized (this) {
      result = null;
    }
    try {
      startTimer();
      connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler, maxrows,
          fetchSize, flags);
    } finally {
      killTimerTask(connection.getQueryExecutor().isRingBufferThreadRunning());
    }
    synchronized (this) {
      checkClosed();
      result = firstUnclosedResult = handler.getResults();

      if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
        generatedKeys = result;
        result = result.getNext();

        if (wantsGeneratedKeysOnce) {
          wantsGeneratedKeysOnce = false;
        }
      }
    }
  }

  public void setCursorName(String name) throws SQLException {
    checkClosed();
    // No-op.
  }

  private volatile boolean isClosed = false;

  @Override
  public int getUpdateCount() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null || result.getResultSet() != null) {
        return -1;
      }

      long count = result.getUpdateCount();
      return count > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : (int) count;
    }
  }

  public boolean getMoreResults() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null) {
        return false;
      }

      result = result.getNext();

      // Close preceding resultsets.
      while (firstUnclosedResult != result) {
        if (firstUnclosedResult.getResultSet() != null) {
          firstUnclosedResult.getResultSet().close();
        }
        firstUnclosedResult = firstUnclosedResult.getNext();
      }

      return (result != null && result.getResultSet() != null);
    }
  }

  public int getMaxRows() throws SQLException {
    checkClosed();
    return maxrows;
  }

  public void setMaxRows(int max) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, max);
  	
    checkClosed();
    if (max < 0) {
      throw new RedshiftException(
          GT.tr("Maximum number of rows must be a value grater than or equal to 0."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
    
    maxrows = max;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, enable);
  	
    checkClosed();
    replaceProcessingEnabled = enable;
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false);
    
  }

  public int getQueryTimeout() throws SQLException {
    checkClosed();
    long seconds = timeout / 1000;
    if (seconds >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) seconds;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, seconds);
  	
    setQueryTimeoutMs(seconds * 1000L);
  }

  /**
   * The queryTimeout limit is the number of milliseconds the driver will wait for a Statement to
   * execute. If the limit is exceeded, a SQLException is thrown.
   *
   * @return the current query timeout limit in milliseconds; 0 = unlimited
   * @throws SQLException if a database access error occurs
   */
  public long getQueryTimeoutMs() throws SQLException {
    checkClosed();
    return timeout;
  }

  /**
   * Sets the queryTimeout limit.
   *
   * @param millis - the new query timeout limit in milliseconds
   * @throws SQLException if a database access error occurs
   */
  public void setQueryTimeoutMs(long millis) throws SQLException {
    checkClosed();

    if (millis < 0) {
      throw new RedshiftException(GT.tr("Query timeout must be a value greater than or equals to 0."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
    timeout = millis;
  }

  /**
   * <p>Either initializes new warning wrapper, or adds warning onto the chain.</p>
   *
   * <p>Although warnings are expected to be added sequentially, the warnings chain may be cleared
   * concurrently at any time via {@link #clearWarnings()}, therefore it is possible that a warning
   * added via this method is placed onto the end of the previous warning chain</p>
   *
   * @param warn warning to add
   */
  public void addWarning(SQLWarning warn) {
    //copy reference to avoid NPE from concurrent modification of this.warnings
    final RedshiftWarningWrapper warnWrap = this.warnings;
    if (warnWrap == null) {
      this.warnings = new RedshiftWarningWrapper(warn);
    } else {
      warnWrap.addWarning(warn);
    }
  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    //copy reference to avoid NPE from concurrent modification of this.warnings
    final RedshiftWarningWrapper warnWrap = this.warnings;
    return warnWrap != null ? warnWrap.getFirstWarning() : null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return maxFieldSize;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, max);
  	
    checkClosed();
    if (max < 0) {
      throw new RedshiftException(
          GT.tr("The maximum field size must be a value greater than or equal to 0."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
    
    maxFieldSize = max;
  }

  /**
   * <p>Clears the warning chain.</p>
   * <p>Note that while it is safe to clear warnings while the query is executing, warnings that are
   * added between calls to {@link #getWarnings()} and #clearWarnings() may be missed.
   * Therefore you should hold a reference to the tail of the previous warning chain
   * and verify if its {@link SQLWarning#getNextWarning()} value is holds any new value.</p>
   */
  public void clearWarnings() throws SQLException {
    warnings = null;
  }

  public ResultSet getResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();

      if (result == null) {
        return null;
      }

      return result.getResultSet();
    }
  }

  /**
   * <B>Note:</B> even though {@code Statement} is automatically closed when it is garbage
   * collected, it is better to close it explicitly to lower resource consumption.
   *
   * {@inheritDoc}
   */
  public final void close() throws SQLException {
  	
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true);
  	
    // closing an already closed Statement is a no-op.
    synchronized (this) {
      if (isClosed) {
      	
        if (RedshiftLogger.isEnable()) 
        	connection.getLogger().logFunction(false);
      	
        return;
      }
      isClosed = true;
    }

    cancel();

    closeForNextExecution();

    closeImpl();
    
    if (RedshiftLogger.isEnable()) {
    	connection.getLogger().logFunction(false);
    	connection.getLogger().flush();
    }
  }

  /**
   * This is guaranteed to be called exactly once even in case of concurrent {@link #close()} calls.
   * @throws SQLException in case of error
   */
  protected void closeImpl() throws SQLException {
    connection.getQueryExecutor().closeStatementAndPortal();      
  }

  /*
   *
   * The following methods are postgres extensions and are defined in the interface BaseStatement
   *
   */

  public long getLastOID() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null) {
        return 0;
      }
      return result.getInsertOID();
    }
  }

  @Override
  public void setPrepareThreshold(int newThreshold) throws SQLException {
    checkClosed();

    if (newThreshold < 0) {
      forceBinaryTransfers = true;
      newThreshold = 1;
    }

    this.mPrepareThreshold = newThreshold;
  }

  @Override
  public int getPrepareThreshold() {
    return mPrepareThreshold;
  }

  @Override
  public void setUseServerPrepare(boolean flag) throws SQLException {
    setPrepareThreshold(flag ? 1 : 0);
  }

  @Override
  public boolean isUseServerPrepare() {
    return false;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed()) {
      throw new RedshiftException(GT.tr("This statement has been closed."),
          RedshiftState.OBJECT_NOT_IN_STATE);
    }
  }

  // ** JDBC 2 Extensions **

  @Override
  public void addBatch(String sql) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql);
  	
    checkClosed();

    if (batchStatements == null) {
      batchStatements = new ArrayList<Query>();
      batchParameters = new ArrayList<ParameterList>();
    }

    // Simple statements should not replace ?, ? with $1, $2
    boolean shouldUseParameterized = false;
    CachedQuery cachedQuery = connection.createQuery(sql, replaceProcessingEnabled, shouldUseParameterized);
    batchStatements.add(cachedQuery.query);
    batchParameters.add(null);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false);
  }

  @Override
  public void clearBatch() throws SQLException {
  	
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true);
  	
    checkClosed();
    
    if (batchStatements != null) {
      batchStatements.clear();
      batchParameters.clear();
    }
    
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(false);
  }

  protected BatchResultHandler createBatchHandler(Query[] queries,
      ParameterList[] parameterLists) {
    return new BatchResultHandler(this, queries, parameterLists,
        wantsGeneratedKeysAlways);
  }

  private BatchResultHandler internalExecuteBatch() throws SQLException {
    // Construct query/parameter arrays.
    transformQueriesAndParameters();
    // Empty arrays should be passed to toArray
    // see http://shipilev.net/blog/2016/arrays-wisdom-ancients/
    Query[] queries = batchStatements.toArray(new Query[0]);
    ParameterList[] parameterLists = batchParameters.toArray(new ParameterList[0]);
    batchStatements.clear();
    batchParameters.clear();

    int flags;

    // Force a Describe before any execution? We need to do this if we're going
    // to send anything dependent on the Describe results, e.g. binary parameters.
    boolean preDescribe = false;

    if (wantsGeneratedKeysAlways) {
      /*
       * This batch will return generated keys, tell the executor to expect result rows. We also
       * force a Describe later so we know the size of the results to expect.
       *
       * If the parameter type(s) change between batch entries and the default binary-mode changes
       * we might get mixed binary and text in a single result set column, which we cannot handle.
       * To prevent this, disable binary transfer mode in batches that return generated keys. See
       * GitHub issue #267
       */
      flags = QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS | QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    } else {
      // If a batch hasn't specified that it wants generated keys, using the appropriate
      // Connection.createStatement(...) interfaces, disallow any result set.
      flags = QueryExecutor.QUERY_NO_RESULTS;
    }

    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    if (preferQueryMode == PreferQueryMode.SIMPLE
        || (preferQueryMode == PreferQueryMode.EXTENDED_FOR_PREPARED
        && parameterLists[0] == null)) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }

    boolean sameQueryAhead = queries.length > 1 && queries[0] == queries[1];

    if (!sameQueryAhead
        // If executing the same query twice in a batch, make sure the statement
        // is server-prepared. In other words, "oneshot" only if the query is one in the batch
        // or the queries are different
        || isOneShotQuery(null)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    } else {
      // If a batch requests generated keys and isn't already described,
      // force a Describe of the query before proceeding. That way we can
      // determine the appropriate size of each batch by estimating the
      // maximum data returned. Without that, we don't know how many queries
      // we'll be able to queue up before we risk a deadlock.
      // (see v3.QueryExecutorImpl's MAX_BUFFERED_RECV_BYTES)

      // SameQueryAhead is just a quick way to issue pre-describe for batch execution
      // TODO: It should be reworked into "pre-describe if query has unknown parameter
      // types and same query is ahead".
      preDescribe = (wantsGeneratedKeysAlways || sameQueryAhead)
          && !queries[0].isStatementDescribed();
      /*
       * It's also necessary to force a Describe on the first execution of the new statement, even
       * though we already described it, to work around bug #267.
       */
      flags |= QueryExecutor.QUERY_FORCE_DESCRIBE_PORTAL;
    }

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }
    if (connection.hintReadOnly()) {
      flags |= QueryExecutor.QUERY_READ_ONLY_HINT;
    }

    BatchResultHandler handler;
    handler = createBatchHandler(queries, parameterLists);

    if ((preDescribe || forceBinaryTransfers)
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Do a client-server round trip, parsing and describing the query so we
      // can determine its result types for use in binary parameters, batch sizing,
      // etc.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler(this);
      try {
        connection.getQueryExecutor().execute(queries[0], parameterLists[0], handler2, 0, 0, flags2);
      } catch (SQLException e) {
        // Unable to parse the first statement -> throw BatchUpdateException
        handler.handleError(e);
        handler.handleCompletion();
        // Will not reach here (see above)
      }
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    synchronized (this) {
      result = null;
    }

    try {
      startTimer();
      connection.getQueryExecutor().execute(queries, parameterLists, handler, maxrows, fetchSize,
          flags);
    } finally {
      killTimerTask(connection.getQueryExecutor().isRingBufferThreadRunning());
      // There might be some rows generated even in case of failures
      synchronized (this) {
        checkClosed();
        if (wantsGeneratedKeysAlways) {
          generatedKeys = new ResultWrapper(handler.getGeneratedKeys());
        }
      }
    }
    return handler;
  }

  public int[] executeBatch() throws SQLException {
  	int[] rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
    checkClosed();
    closeForNextExecution();

    if (batchStatements == null || batchStatements.isEmpty()) {
      rc = new int[0];
    }
    else
    	rc = internalExecuteBatch().getUpdateCount();

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public void cancel() throws SQLException {
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
    if (statementState == StatementCancelState.IDLE) {
    	
      if (RedshiftLogger.isEnable()) {
      	connection.getLogger().logError("statementState is StatementCancelState.IDLE");
      	connection.getLogger().logFunction(false);
      }
    	
      return;
    }
    if (!STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY,
        StatementCancelState.CANCELING)) {
      // Not in query, there's nothing to cancel
    	
      if (RedshiftLogger.isEnable()) {
      	connection.getLogger().logError("statementState is not StatementCancelState.IN_QUERY");
      	connection.getLogger().logFunction(false);
      }
    	
      return;
    }
    // Synchronize on connection to avoid spinning in killTimerTask
    synchronized (connection) {
      try {
        connection.cancelQuery();
      } finally {
        STATE_UPDATER.set(this, StatementCancelState.CANCELLED);
        connection.notifyAll(); // wake-up killTimerTask
      }
    }
    
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(false);
  }

  public Connection getConnection() throws SQLException {
    checkClosed();
  	
    return connection;
  }

  public int getFetchDirection() throws SQLException {
    checkClosed();
    return fetchdirection;
  }

  public int getResultSetConcurrency() throws SQLException {
    checkClosed();
    return concurrency;
  }

  public int getResultSetType() throws SQLException {
    checkClosed();
    return resultsettype;
  }

  public void setFetchDirection(int direction) throws SQLException {
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, direction);
  	
    switch (direction) {
      case ResultSet.FETCH_FORWARD:
      case ResultSet.FETCH_REVERSE:
      case ResultSet.FETCH_UNKNOWN:
        fetchdirection = direction;
        break;
      default:
        throw new RedshiftException(GT.tr("Invalid fetch direction constant: {0}.", direction),
            RedshiftState.INVALID_PARAMETER_VALUE);
    }
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false);
    
  }

  public void setFetchSize(int rows) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, rows);
  	
    checkClosed();
    
    if (rows < 0) {
      throw new RedshiftException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
    
    fetchSize = rows;
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false);
  }

  private void startTimer() {
    /*
     * there shouldn't be any previous timer active, but better safe than sorry.
     */
    cleanupTimer();

    STATE_UPDATER.set(this, StatementCancelState.IN_QUERY);

    if (timeout == 0) {
      return;
    }

    TimerTask cancelTask = new TimerTask() {
      public void run() {
        try {
          if (!CANCEL_TIMER_UPDATER.compareAndSet(RedshiftStatementImpl.this, this, null)) {
            // Nothing to do here, statement has already finished and cleared
            // cancelTimerTask reference
            return;
          }
          RedshiftStatementImpl.this.cancel();
        } catch (SQLException e) {
        }
      }
    };

    CANCEL_TIMER_UPDATER.set(this, cancelTask);
    connection.addTimerTask(cancelTask, timeout);
  }

  /**
   * Clears {@link #cancelTimerTask} if any. Returns true if and only if "cancel" timer task would
   * never invoke {@link #cancel()}.
   */
  private boolean cleanupTimer() {
    TimerTask timerTask = CANCEL_TIMER_UPDATER.get(this);
    if (timerTask == null) {
      // If timeout is zero, then timer task did not exist, so we safely report "all clear"
      return timeout == 0;
    }
    if (!CANCEL_TIMER_UPDATER.compareAndSet(this, timerTask, null)) {
      // Failed to update reference -> timer has just fired, so we must wait for the query state to
      // become "cancelling".
      return false;
    }
    timerTask.cancel();
    connection.purgeTimerTasks();
    // All clear
    return true;
  }

  private void killTimerTask(boolean isRingBufferThreadRunning) {
    boolean timerTaskIsClear = cleanupTimer();
    // The order is important here: in case we need to wait for the cancel task, the state must be
    // kept StatementCancelState.IN_QUERY, so cancelTask would be able to cancel the query.
    // It is believed that this case is very rare, so "additional cancel and wait below" would not
    // harm it.
    if (timerTaskIsClear && isRingBufferThreadRunning)
    		return;
    if (timerTaskIsClear && STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.IDLE)) {
      return;
    }
    
    if (timerTaskIsClear && (STATE_UPDATER.get(this) == StatementCancelState.IDLE))
    	return;
    

    // Being here means someone managed to call .cancel() and our connection did not receive
    // "timeout error"
    // We wait till state becomes "cancelled"
    boolean interrupted = false;
    synchronized (connection) {
      // state check is performed under synchronized so it detects "cancelled" state faster
      // In other words, it prevents unnecessary ".wait()" call
      while (!STATE_UPDATER.compareAndSet(this, StatementCancelState.CANCELLED, StatementCancelState.IDLE)) {
        try {
          // Note: wait timeout here is irrelevant since synchronized(connection) would block until
          // .cancel finishes
          connection.wait(10);
        } catch (InterruptedException e) { // NOSONAR
          // Either re-interrupt this method or rethrow the "InterruptedException"
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  protected boolean getForceBinaryTransfer() {
    return forceBinaryTransfers;
  }

  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
  @Override
  public long getLargeUpdateCount() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null || result.getResultSet() != null) {
        return -1;
      }

      return result.getUpdateCount();
    }
  }

  public void setLargeMaxRows(long max) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setLargeMaxRows");
  }

  public long getLargeMaxRows() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getLargeMaxRows");
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
  	long[] rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
    checkClosed();
    closeForNextExecution();

    if (batchStatements == null || batchStatements.isEmpty()) {
      rc = new long[0];
    }
    else
    	rc = internalExecuteBatch().getLargeUpdateCount();

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql);
  	
    executeWithFlags(sql, QueryExecutor.QUERY_NO_RESULTS);
    checkNoResultUpdate();
    long rc = getLargeUpdateCount();

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
  	long rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql);
  	
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      rc = executeLargeUpdate(sql);
    }
    else
    	rc = executeLargeUpdate(sql, (String[]) null);
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql, columnIndexes);
  	
    if (columnIndexes == null || columnIndexes.length == 0) {
      long rc = executeLargeUpdate(sql);

      if (RedshiftLogger.isEnable())
      	connection.getLogger().logFunction(false, rc);
      
      return rc;
    }

    throw new RedshiftException(GT.tr("Returning autogenerated keys by column index is not supported."),
        RedshiftState.NOT_IMPLEMENTED);
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
  	long rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql, columnNames);
  	
    if (columnNames == null || columnNames.length == 0) {
      rc = executeLargeUpdate(sql);
    }
    else {
      throw new RedshiftException(GT.tr("Returning autogenerated keys by column name is not supported."),
          RedshiftState.NOT_IMPLEMENTED);
    	
/*	    wantsGeneratedKeysOnce = true;
	    if (!executeCachedSql(sql, 0, columnNames)) {
	      // no resultset returned. What's a pity!
	    }
	    rc = getLargeUpdateCount(); */
    }
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }
  //JCP! endif

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public void setPoolable(boolean poolable) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, poolable);
  	
    checkClosed();
    this.poolable = poolable;
  }

  public boolean isPoolable() throws SQLException {
    checkClosed();
    return poolable;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public void closeOnCompletion() throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true);
  	
    checkClosed();
    
    closeOnCompletion = true;
    
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(false);
  }

  public boolean isCloseOnCompletion() throws SQLException {
    checkClosed();
  	
    return closeOnCompletion;
  }

  protected void checkCompletion() throws SQLException {
    if (!closeOnCompletion) {
      return;
    }

    synchronized (this) {
      ResultWrapper result = firstUnclosedResult;
      while (result != null) {
        if (result.getResultSet() != null && !result.getResultSet().isClosed()) {
          return;
        }
        result = result.getNext();
      }
    }

    // prevent all ResultSet.close arising from Statement.close to loop here
    closeOnCompletion = false;
    try {
      close();
    } finally {
      // restore the status if one rely on isCloseOnCompletion
      closeOnCompletion = true;
    }
  }

  public boolean getMoreResults(int current) throws SQLException {
    synchronized (this) {
      checkClosed();
      // CLOSE_CURRENT_RESULT
      if (current == Statement.CLOSE_CURRENT_RESULT && result != null
          && result.getResultSet() != null) {
        result.getResultSet().close();
      }

      // Advance resultset.
      if (result != null) {
        result = result.getNext();
      }

      // CLOSE_ALL_RESULTS
      if (current == Statement.CLOSE_ALL_RESULTS) {
        // Close preceding resultsets.
        while (firstUnclosedResult != result) {
          if (firstUnclosedResult.getResultSet() != null) {
            firstUnclosedResult.getResultSet().close();
          }
          firstUnclosedResult = firstUnclosedResult.getNext();
        }
      }

      // Done.
      return (result != null && result.getResultSet() != null);
    }
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (generatedKeys == null || generatedKeys.getResultSet() == null) {
        return createDriverResultSet(new Field[0], new ArrayList<Tuple>());
      }

      return generatedKeys.getResultSet();
    }
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
  	int rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql, autoGeneratedKeys);
  	
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      rc = executeUpdate(sql);
    }
    else {
	    rc =  executeUpdate(sql, (String[]) null);
    }
    
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql, columnIndexes);
  	
    if (columnIndexes == null || columnIndexes.length == 0) {
    	int rc = executeUpdate(sql);
    	
      if (RedshiftLogger.isEnable())
      	connection.getLogger().logFunction(false, rc);
      
      return rc;
    }

    throw new RedshiftException(GT.tr("Returning autogenerated keys by column index is not supported."),
        RedshiftState.NOT_IMPLEMENTED);
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
  	int rc;
  	
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, sql, columnNames);
  	
    if (columnNames == null
    		|| columnNames.length == 0) {
      rc = executeUpdate(sql);
    }
    else {
      throw new RedshiftException(GT.tr("Returning autogenerated keys by column name is not supported."),
          RedshiftState.NOT_IMPLEMENTED);
    	
/*	    wantsGeneratedKeysOnce = true;
	    if (!executeCachedSql(sql, 0, columnNames)) {
	      // no resultset returned. What's a pity!
	    }
	    rc = getUpdateCount(); */
    }

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public boolean execute(String sql, int autoGeneratedKeys) 
  				throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, sql, autoGeneratedKeys);
  	
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    }
    return execute(sql, (String[]) null);
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, sql, columnIndexes);
  	
    if (columnIndexes != null && columnIndexes.length == 0) {
      return execute(sql);
    }

    throw new RedshiftException(GT.tr("Returning autogenerated keys by column index is not supported."),
        RedshiftState.NOT_IMPLEMENTED);
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
  	boolean rc;
  	
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, sql, columnNames);
  	
    if (columnNames == null || columnNames.length == 0) {
      rc = execute(sql);
    }
    else {
      throw new RedshiftException(GT.tr("Returning autogenerated keys by column name is not supported."),
          RedshiftState.NOT_IMPLEMENTED);
    	
/*	    wantsGeneratedKeysOnce = true;
	    rc = executeCachedSql(sql, 0, columnNames); */
    }

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  public int getResultSetHoldability() throws SQLException {
    checkClosed();
  	
    return rsHoldability;
  }

  public ResultSet createDriverResultSet(Field[] fields, List<Tuple> tuples)
      throws SQLException {
    return createResultSet(null, fields, tuples, null, null, null, null);
  }

  protected void transformQueriesAndParameters() throws SQLException {
  }

}
