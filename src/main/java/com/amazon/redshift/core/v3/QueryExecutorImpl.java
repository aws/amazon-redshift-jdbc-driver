/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core.v3;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.copy.CopyIn;
import com.amazon.redshift.copy.CopyOperation;
import com.amazon.redshift.copy.CopyOut;
import com.amazon.redshift.core.CommandCompleteParser;
import com.amazon.redshift.core.Encoding;
import com.amazon.redshift.core.EncodingPredictor;
import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.NativeQuery;
import com.amazon.redshift.core.Oid;
import com.amazon.redshift.core.RedshiftBindException;
import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.core.ParameterList;
import com.amazon.redshift.core.Parser;
import com.amazon.redshift.core.Query;
import com.amazon.redshift.core.QueryExecutor;
import com.amazon.redshift.core.QueryExecutorBase;
import com.amazon.redshift.core.ReplicationProtocol;
import com.amazon.redshift.core.ResultCursor;
import com.amazon.redshift.core.ResultHandler;
import com.amazon.redshift.core.ResultHandlerBase;
import com.amazon.redshift.core.ResultHandlerDelegate;
import com.amazon.redshift.core.SqlCommand;
import com.amazon.redshift.core.SqlCommandType;
import com.amazon.redshift.core.TransactionState;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.core.v3.replication.V3ReplicationProtocol;
import com.amazon.redshift.jdbc.AutoSave;
import com.amazon.redshift.jdbc.BatchResultHandler;
import com.amazon.redshift.jdbc.FieldMetadata;
import com.amazon.redshift.jdbc.TimestampUtils;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

import com.amazon.redshift.util.QuerySanitizer;
import com.amazon.redshift.util.ByteStreamWriter;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftPropertyMaxResultBufferParser;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.RedshiftWarning;

import com.amazon.redshift.util.ServerErrorMessage;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * QueryExecutor implementation for the V3 protocol.
 */
public class QueryExecutorImpl extends QueryExecutorBase {
  private static final String COPY_ERROR_MESSAGE = "COPY commands are only supported using the CopyManager API.";
  private static final Pattern ROLLBACK_PATTERN = Pattern.compile("\\brollback\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern COMMIT_PATTERN = Pattern.compile("\\bcommit\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern PREPARE_PATTERN = Pattern.compile("\\bprepare ++transaction\\b", Pattern.CASE_INSENSITIVE);

  private static boolean looksLikeCommit(String sql) {
    if ("COMMIT".equalsIgnoreCase(sql)) {
      return true;
    }
    if ("ROLLBACK".equalsIgnoreCase(sql)) {
      return false;
    }
    return COMMIT_PATTERN.matcher(sql).find() && !ROLLBACK_PATTERN.matcher(sql).find();
  }

  private static boolean looksLikePrepare(String sql) {
    return sql.startsWith("PREPARE TRANSACTION") || PREPARE_PATTERN.matcher(sql).find();
  }

  /**
   * TimeZone of the current connection (TimeZone backend parameter).
   */
  private TimeZone timeZone;

  /**
   * application_name connection property.
   */
  private String applicationName;

  /**
   * True if server uses integers for date and time fields. False if server uses double.
   */
  private boolean integerDateTimes;

  /**
   * Bit set that has a bit set for each oid which should be received using binary format.
   */
  private final Set<Integer> useBinaryReceiveForOids = new HashSet<Integer>();

  /**
   * Bit set that has a bit set for each oid which should be sent using binary format.
   */
  private final Set<Integer> useBinarySendForOids = new HashSet<Integer>();

  /**
   * This is a fake query object so processResults can distinguish "ReadyForQuery" messages
   * from Sync messages vs from simple execute (aka 'Q').
   */
  private final SimpleQuery sync = (SimpleQuery) createQuery("SYNC", false, true).query;

  private short deallocateEpoch;

  /**
   * This caches the latest observed {@code set search_path} query so the reset of prepared
   * statement cache can be skipped if using repeated calls for the same {@code set search_path}
   * value.
   */
  private String lastSetSearchPathQuery;

  /**
   * The exception that caused the last transaction to fail.
   */
  private SQLException transactionFailCause;

  private final ReplicationProtocol replicationProtocol;
  
  private boolean enableFetchRingBuffer;
  
  private long fetchRingBufferSize;
  
  // Last running ring buffer thread.
  private RingBufferThread m_ringBufferThread = null;
  private boolean m_ringBufferStopThread = false;
  private Object m_ringBufferThreadLock = new Object();
  
  // Query or some execution on a socket in process
  private final Lock m_executingLock = new ReentrantLock();

  /**
   * {@code CommandComplete(B)} messages are quite common, so we reuse instance to parse those
   */
  private final CommandCompleteParser commandCompleteParser = new CommandCompleteParser();
  private final CopyQueryExecutor copyQueryExecutor;
  
  public QueryExecutorImpl(RedshiftStream pgStream, String user, String database,
      int cancelSignalTimeout, Properties info, RedshiftLogger logger) throws SQLException, IOException {
    super(pgStream, user, database, cancelSignalTimeout, info, logger);

    this.allowEncodingChanges = RedshiftProperty.ALLOW_ENCODING_CHANGES.getBoolean(info);
    this.cleanupSavePoints = RedshiftProperty.CLEANUP_SAVEPOINTS.getBoolean(info);
    this.replicationProtocol = new V3ReplicationProtocol(this, pgStream);
    this.enableFetchRingBuffer = RedshiftProperty.ENABLE_FETCH_RING_BUFFER.getBoolean(info);
    String fetchRingBufferSizeStr = RedshiftProperty.FETCH_RING_BUFFER_SIZE.get(info);
    this.fetchRingBufferSize = (fetchRingBufferSizeStr != null ) 
    															? RedshiftPropertyMaxResultBufferParser.parseProperty(fetchRingBufferSizeStr, RedshiftProperty.FETCH_RING_BUFFER_SIZE.getName())
    															: 0;

    this.enableStatementCache = RedshiftProperty.ENABLE_STATEMENT_CACHE.getBoolean(info);
    this.copyQueryExecutor = new CopyQueryExecutor(this, logger, pgStream);
    this.serverProtocolVersion = 0;
    readStartupMessages();
  }

  @Override
  public int getProtocolVersion() {
    return 3;
  }

  public long getBytesReadFromStream()
  {
      return pgStream.getBytesFromStream();
  }

  /**
   * <p>Supplement to synchronization of public methods on current QueryExecutor.</p>
   *
   * <p>Necessary for keeping the connection intact between calls to public methods sharing a state
   * such as COPY subprotocol. waitOnLock() must be called at beginning of each connection access
   * point.</p>
   *
   * <p>Public methods sharing that state must then be synchronized among themselves. Normal method
   * synchronization typically suffices for that.</p>
   *
   * <p>See notes on related methods as well as currentCopy() below.</p>
   */
  private Object lockedFor = null;

  /**
   * Obtain lock over this connection for given object, blocking to wait if necessary.
   *
   * @param obtainer object that gets the lock. Normally current thread.
   * @throws RedshiftException when already holding the lock or getting interrupted.
   */
  void lock(Object obtainer) throws RedshiftException {
    if (lockedFor == obtainer) {
      throw new RedshiftException(GT.tr("Tried to obtain lock while already holding it"),
          RedshiftState.OBJECT_NOT_IN_STATE);

    }
    waitOnLock();
    lockedFor = obtainer;
  }

  /**
   * Release lock on this connection presumably held by given object.
   *
   * @param holder object that holds the lock. Normally current thread.
   * @throws RedshiftException when this thread does not hold the lock
   */
  void unlock(Object holder) throws RedshiftException {
    if (lockedFor != holder) {
      throw new RedshiftException(GT.tr("Tried to break lock on database connection"),
          RedshiftState.OBJECT_NOT_IN_STATE);
    }
    lockedFor = null;
    this.notify();
  }

  /**
   * Wait until our lock is released. Execution of a single synchronized method can then continue
   * without further ado. Must be called at beginning of each synchronized public method.
   */
  void waitOnLock() throws RedshiftException {
    while (lockedFor != null) {
      try {
        this.wait();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RedshiftException(
            GT.tr("Interrupted while waiting to obtain lock on database connection"),
            RedshiftState.OBJECT_NOT_IN_STATE, ie);
      }
    }
  }

  /**
   * @param holder object assumed to hold the lock
   * @return whether given object actually holds the lock
   */
  boolean hasLock(Object holder) {
    return lockedFor == holder;
  }

  //
  // Query parsing
  //

  public Query createSimpleQuery(String sql) throws SQLException {
    List<NativeQuery> queries = Parser.parseJdbcSql(sql,
        getStandardConformingStrings(), false, true, true,
        isReWriteBatchedInsertsEnabled());
    return wrap(queries);
  }

  @Override
  public Query wrap(List<NativeQuery> queries) {
    if (queries.isEmpty()) {
      // Empty query
      return emptyQuery;
    }
    if (queries.size() == 1) {
      NativeQuery firstQuery = queries.get(0);
      if (isReWriteBatchedInsertsEnabled()
          && firstQuery.getCommand().isBatchedReWriteCompatible()) {
        int valuesBraceOpenPosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceOpenPosition();
        int valuesBraceClosePosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceClosePosition();
        return new BatchedQuery(firstQuery, this, valuesBraceOpenPosition,
            valuesBraceClosePosition, isColumnSanitiserDisabled(), logger);
      } else {
        return new SimpleQuery(firstQuery, this, isColumnSanitiserDisabled(), logger);
      }
    }

    // Multiple statements.
    SimpleQuery[] subqueries = new SimpleQuery[queries.size()];
    int[] offsets = new int[subqueries.length];
    int offset = 0;
    for (int i = 0; i < queries.size(); ++i) {
      NativeQuery nativeQuery = queries.get(i);
      offsets[i] = offset;
      subqueries[i] = new SimpleQuery(nativeQuery, this, isColumnSanitiserDisabled(), logger);
      offset += nativeQuery.bindPositions.length;
    }

    return new CompositeQuery(subqueries, offsets);
  }

  //
  // Query execution
  //

  private int updateQueryMode(int flags) {
    switch (getPreferQueryMode()) {
      case SIMPLE:
        return flags | QUERY_EXECUTE_AS_SIMPLE;
      case EXTENDED:
        return flags & ~QUERY_EXECUTE_AS_SIMPLE;
      default:
        return flags;
    }
  }

  public  void execute(Query query, ParameterList parameters, ResultHandler handler,
      int maxRows, int fetchSize, int flags) throws SQLException {
    // Wait for current ring buffer thread to finish, if any.
  	// Shouldn't call from synchronized method, which can cause dead-lock.
    waitForRingBufferThreadToFinish(false, false, false, null, null);
    
    synchronized(this) {
	  	waitOnLock();
	  	try {
	  		m_executingLock.lock();	  		
		    if (RedshiftLogger.isEnable()) {
		      logger.log(LogLevel.DEBUG, "  simple execute, handler={0}, maxRows={1}, fetchSize={2}, flags={3}",
		          new Object[]{handler, maxRows, fetchSize, flags});
		    }
		
		    if (handler != null) {
		    	handler.setStatementStateInQueryFromIdle();	    	
		    }
		    
		    if (parameters == null) {
		      parameters = SimpleQuery.NO_PARAMETERS;
		    }
		
		    flags = updateQueryMode(flags);
		
		    boolean describeOnly = (QUERY_DESCRIBE_ONLY & flags) != 0;
		
		    ((V3ParameterList) parameters).convertFunctionOutParameters();
		
		    // Check parameters are all set..
		    if (!describeOnly) {
		      ((V3ParameterList) parameters).checkAllParametersSet();
		    }
		
		    boolean autosave = false;
		    try {
		      try {
		        handler = sendQueryPreamble(handler, flags);
		        autosave = sendAutomaticSavepoint(query, flags);
		        sendQuery(query, (V3ParameterList) parameters, maxRows, fetchSize, flags,
		            handler, null);
		        if ((flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0) {
		          // Sync message is not required for 'Q' execution as 'Q' ends with ReadyForQuery message
		          // on its own
		        } else {
		        	sendFlush();
		          sendSync(true);
		        }
		        processResults(handler, flags, fetchSize, (query.getSubqueries() != null), maxRows);
		        estimatedReceiveBufferBytes = 0;
		      } catch (RedshiftBindException se) {
		        // There are three causes of this error, an
		        // invalid total Bind message length, a
		        // BinaryStream that cannot provide the amount
		        // of data claimed by the length argument, and
		        // a BinaryStream that throws an Exception
		        // when reading.
		        //
		        // We simply do not send the Execute message
		        // so we can just continue on as if nothing
		        // has happened. Perhaps we need to
		        // introduce an error here to force the
		        // caller to rollback if there is a
		        // transaction in progress?
		        //
		        sendSync(true);
		        processResults(handler, flags, 0, (query.getSubqueries() != null), maxRows);
		        estimatedReceiveBufferBytes = 0;
		        handler
		            .handleError(new RedshiftException(GT.tr("Unable to bind parameter values for statement."),
		                RedshiftState.INVALID_PARAMETER_VALUE, se.getIOException(), logger));
		      }
		    } catch (IOException e) {
		      abort();
		      handler.handleError(
		          new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
		              RedshiftState.CONNECTION_FAILURE, e, logger));
		    } catch (SQLException sqe) {
		      if(RedshiftLogger.isEnable())
		      	logger.logError(sqe);
		    	
		    	throw sqe;
		    }
		
		    try {
		      handler.handleCompletion();
		      if (cleanupSavePoints) {
		        releaseSavePoint(autosave, flags);
		      }
		    } catch (SQLException e) {
		      rollbackIfRequired(autosave, e);
		    }
	  	} 
	  	finally {
	  		m_executingLock.unlock();
	  	}
    } // synchronized
  }

  private boolean sendAutomaticSavepoint(Query query, int flags) throws IOException {
    if (((flags & QueryExecutor.QUERY_SUPPRESS_BEGIN) == 0
        || getTransactionState() == TransactionState.OPEN)
        && query != restoreToAutoSave
        && getAutoSave() != AutoSave.NEVER
        // If query has no resulting fields, it cannot fail with 'cached plan must not change result type'
        // thus no need to set a savepoint before such query
        && (getAutoSave() == AutoSave.ALWAYS
        // If CompositeQuery is observed, just assume it might fail and set the savepoint
        || !(query instanceof SimpleQuery)
        || ((SimpleQuery) query).getFields() != null)) {

      /*
      create a different SAVEPOINT the first time so that all subsequent SAVEPOINTS can be released
      easily. There have been reports of server resources running out if there are too many
      SAVEPOINTS.
       */
      sendOneQuery(autoSaveQuery, SimpleQuery.NO_PARAMETERS, 1, 0,
          QUERY_NO_RESULTS | QUERY_NO_METADATA
              // Redshift does not support bind, exec, simple, sync message flow,
              // so we force autosavepoint to use simple if the main query is using simple
              | QUERY_EXECUTE_AS_SIMPLE);
      return true;
    }
    return false;
  }

  private void releaseSavePoint(boolean autosave, int flags) throws SQLException {
    if ( autosave
        && getAutoSave() == AutoSave.ALWAYS
        && getTransactionState() == TransactionState.OPEN) {
      try {
        sendOneQuery(releaseAutoSave, SimpleQuery.NO_PARAMETERS, 1, 0,
            QUERY_NO_RESULTS | QUERY_NO_METADATA
                | QUERY_EXECUTE_AS_SIMPLE);

      } catch (IOException ex) {
        throw  new RedshiftException(GT.tr("Error releasing savepoint"), RedshiftState.IO_ERROR);
      }
    }
  }

  private void rollbackIfRequired(boolean autosave, SQLException e) throws SQLException {
    if (autosave
        && getTransactionState() == TransactionState.FAILED
        && (getAutoSave() == AutoSave.ALWAYS || willHealOnRetry(e))) {
      try {
        // ROLLBACK and AUTOSAVE are executed as simple always to overcome "statement no longer exists S_xx"
        execute(restoreToAutoSave, SimpleQuery.NO_PARAMETERS, new ResultHandlerDelegate(null),
            1, 0, QUERY_NO_RESULTS | QUERY_NO_METADATA | QUERY_EXECUTE_AS_SIMPLE);
      } catch (SQLException e2) {
        // That's O(N), sorry
        e.setNextException(e2);
      }
    }
    
    if(RedshiftLogger.isEnable())
    	logger.logError(e);
    
    throw e;
  }

  // Deadlock avoidance:
  //
  // It's possible for the send and receive streams to get "deadlocked" against each other since
  // we do not have a separate thread. The scenario is this: we have two streams:
  //
  // driver -> TCP buffering -> server
  // server -> TCP buffering -> driver
  //
  // The server behaviour is roughly:
  // while true:
  // read message
  // execute message
  // write results
  //
  // If the server -> driver stream has a full buffer, the write will block.
  // If the driver is still writing when this happens, and the driver -> server
  // stream also fills up, we deadlock: the driver is blocked on write() waiting
  // for the server to read some more data, and the server is blocked on write()
  // waiting for the driver to read some more data.
  //
  // To avoid this, we guess at how much response data we can request from the
  // server before the server -> driver stream's buffer is full (MAX_BUFFERED_RECV_BYTES).
  // This is the point where the server blocks on write and stops reading data. If we
  // reach this point, we force a Sync message and read pending data from the server
  // until ReadyForQuery, then go back to writing more queries unless we saw an error.
  //
  // This is not 100% reliable -- it's only done in the batch-query case and only
  // at a reasonably high level (per query, not per message), and it's only an estimate
  // -- so it might break. To do it correctly in all cases would seem to require a
  // separate send or receive thread as we can only do the Sync-and-read-results
  // operation at particular points, and also as we don't really know how much data
  // the server is sending.
  //
  // Our message size estimation is coarse, and disregards asynchronous
  // notifications, warnings/info/debug messages, etc, so the response size may be
  // quite different from the 250 bytes assumed here even for queries that don't
  // return data.
  //
  // See github issue #194 and #195 .
  //
  // Assume 64k server->client buffering, which is extremely conservative. A typical
  // system will have 200kb or more of buffers for its receive buffers, and the sending
  // system will typically have the same on the send side, giving us 400kb or to work
  // with. (We could check Java's receive buffer size, but prefer to assume a very
  // conservative buffer instead, and we don't know how big the server's send
  // buffer is.)
  //
  private static final int MAX_BUFFERED_RECV_BYTES = 64000;
  private static final int NODATA_QUERY_RESPONSE_SIZE_BYTES = 250;

  public  void execute(Query[] queries, ParameterList[] parameterLists,
      BatchResultHandler batchHandler, int maxRows, int fetchSize, int flags) throws SQLException {
    
  	// Wait for current ring buffer thread to finish, if any.
  	// Shouldn't call from synchronized method, which can cause dead-lock.
    waitForRingBufferThreadToFinish(false, false, false, null, null);
  	
    synchronized(this) {
	    waitOnLock();
	    try {
	    	m_executingLock.lock();
	    	
		    if (RedshiftLogger.isEnable()) {
		      logger.log(LogLevel.DEBUG, "  batch execute {0} queries, handler={1}, maxRows={2}, fetchSize={3}, flags={4}",
		          new Object[]{queries.length, batchHandler, maxRows, fetchSize, flags});
		    }
		
		    if (batchHandler != null) {
		    	batchHandler.setStatementStateInQueryFromIdle();	    	
		    }
		    
		    flags = updateQueryMode(flags);
		
		    boolean describeOnly = (QUERY_DESCRIBE_ONLY & flags) != 0;
		    // Check parameters and resolve OIDs.
		    if (!describeOnly) {
		      for (ParameterList parameterList : parameterLists) {
		        if (parameterList != null) {
		          ((V3ParameterList) parameterList).checkAllParametersSet();
		        }
		      }
		    }
		
		    boolean autosave = false;
		    ResultHandler handler = batchHandler;
		    try {
		      handler = sendQueryPreamble(batchHandler, flags);
		      autosave = sendAutomaticSavepoint(queries[0], flags);
		      estimatedReceiveBufferBytes = 0;
		
		      for (int i = 0; i < queries.length; ++i) {
		        Query query = queries[i];
		        V3ParameterList parameters = (V3ParameterList) parameterLists[i];
		        if (parameters == null) {
		          parameters = SimpleQuery.NO_PARAMETERS;
		        }
		
		        sendQuery(query, parameters, maxRows, fetchSize, flags, handler, batchHandler);
		
		        if (handler.getException() != null) {
		          break;
		        }
		      }
		
		      if (handler.getException() == null) {
		        if ((flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0) {
		          // Sync message is not required for 'Q' execution as 'Q' ends with ReadyForQuery message
		          // on its own
		        } else {
		        	sendFlush();
		          sendSync(true);
		        }
		        processResults(handler, flags, fetchSize, true, maxRows);
		        estimatedReceiveBufferBytes = 0;
		      }
		    } catch (IOException e) {
		      abort();
		      handler.handleError(
		          new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
		              RedshiftState.CONNECTION_FAILURE, e, logger));
		    } catch (SQLException sqe) {
			      if(RedshiftLogger.isEnable())
			      	logger.logError(sqe);
			    	
			    	throw sqe;
		    }
	
		    try {
		      handler.handleCompletion();
		      if (cleanupSavePoints) {
		        releaseSavePoint(autosave, flags);
		      }
		    } catch (SQLException e) {
		      rollbackIfRequired(autosave, e);
		    }
	    }
	    finally {
	    	m_executingLock.unlock();	    	
	    }
    } // synchronized
  }

  private ResultHandler sendQueryPreamble(final ResultHandler delegateHandler, int flags)
      throws IOException {
    // First, send CloseStatements for finalized SimpleQueries that had statement names assigned.
    processDeadParsedQueries();
    processDeadPortals();

    // Send BEGIN on first statement in transaction.
    if ((flags & QueryExecutor.QUERY_SUPPRESS_BEGIN) != 0
        || getTransactionState() != TransactionState.IDLE) {
      return delegateHandler;
    }

    int beginFlags = QueryExecutor.QUERY_NO_METADATA;
    if ((flags & QueryExecutor.QUERY_ONESHOT) != 0) {
      beginFlags |= QueryExecutor.QUERY_ONESHOT;
    }

    beginFlags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;

    beginFlags = updateQueryMode(beginFlags);

    final SimpleQuery beginQuery = ((flags & QueryExecutor.QUERY_READ_ONLY_HINT) == 0) ? beginTransactionQuery : beginReadOnlyTransactionQuery;

    sendOneQuery(beginQuery, SimpleQuery.NO_PARAMETERS, 0, 0, beginFlags);

    // Insert a handler that intercepts the BEGIN.
    return new ResultHandlerDelegate(delegateHandler) {
      private boolean sawBegin = false;

      public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
          ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
          int[] rowCount, Thread ringBufferThread) {
        if (sawBegin) {
          super.handleResultRows(fromQuery, fields, tuples, cursor, queueTuples, rowCount, ringBufferThread);
        }
      }

      @Override
      public void handleCommandStatus(String status, long updateCount, long insertOID) {
        if (!sawBegin) {
          sawBegin = true;
          if (!status.equals("BEGIN")) {
            handleError(new RedshiftException(GT.tr("Expected command status BEGIN, got {0}.", status),
                RedshiftState.PROTOCOL_VIOLATION));
          }
        } else {
          super.handleCommandStatus(status, updateCount, insertOID);
        }
      }
    }; 
  }

  //
  // Fastpath
  //

  public byte[] fastpathCall(int fnid, ParameterList parameters, boolean suppressBegin)
      throws SQLException {
  	return null;
/* Not in use. TODO: Comment all references used in LargeObject.
    // Wait for current ring buffer thread to finish, if any.
  	// Shouldn't call from synchronized method, which can cause dead-lock.
    waitForRingBufferThreadToFinish(false, false, null, null);
    
    synchronized(this) {
	    waitOnLock();
	    
	    if (!suppressBegin) {
	      doSubprotocolBegin();
	    }
	    try {
	      sendFastpathCall(fnid, (SimpleParameterList) parameters);
	      return receiveFastpathResult();
	    } catch (IOException ioe) {
	      abort();
	      throw new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
    }
*/    
  }

  public void doSubprotocolBegin() throws SQLException {
    if (getTransactionState() == TransactionState.IDLE) {

    	if(RedshiftLogger.isEnable())    	
      	logger.log(LogLevel.DEBUG, "Issuing BEGIN before fastpath or copy call.");

      ResultHandler handler = new ResultHandlerBase() {
        private boolean sawBegin = false;

        @Override
        public void handleCommandStatus(String status, long updateCount, long insertOID) {
          if (!sawBegin) {
            if (!status.equals("BEGIN")) {
              handleError(
                  new RedshiftException(GT.tr("Expected command status BEGIN, got {0}.", status),
                      RedshiftState.PROTOCOL_VIOLATION));
            }
            sawBegin = true;
          } else {
            handleError(new RedshiftException(GT.tr("Unexpected command status: {0}.", status),
                RedshiftState.PROTOCOL_VIOLATION));
          }
        }

        @Override
        public void handleWarning(SQLWarning warning) {
          // we don't want to ignore warnings and it would be tricky
          // to chain them back to the connection, so since we don't
          // expect to get them in the first place, we just consider
          // them errors.
          handleError(warning);
        }
      };

      try {
        /* Send BEGIN with simple protocol preferred */
        int beginFlags = QueryExecutor.QUERY_NO_METADATA
                         | QueryExecutor.QUERY_ONESHOT
                         | QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
        beginFlags = updateQueryMode(beginFlags);
        sendOneQuery(beginTransactionQuery, SimpleQuery.NO_PARAMETERS, 0, 0, beginFlags);
        sendSync(true);
        processResults(handler, 0, 0, false, 0);
        estimatedReceiveBufferBytes = 0;
      } catch (IOException ioe) {
        throw new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
            RedshiftState.CONNECTION_FAILURE, ioe);
      }
    }

  }

  public ParameterList createFastpathParameters(int count) {
    return new SimpleParameterList(count, this);
  }

/* Not in use.  
  private void sendFastpathCall(int fnid, SimpleParameterList params)
      throws SQLException, IOException {
    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " FE=> FunctionCall({0}, {1} params)", new Object[]{fnid, params.getParameterCount()});
    }

    //
    // Total size = 4 (length)
    // + 4 (function OID)
    // + 2 (format code count) + N * 2 (format codes)
    // + 2 (parameter count) + encodedSize (parameters)
    // + 2 (result format)

    int paramCount = params.getParameterCount();
    int encodedSize = 0;
    for (int i = 1; i <= paramCount; ++i) {
      if (params.isNull(i)) {
        encodedSize += 4;
      } else {
        encodedSize += 4 + params.getV3Length(i);
      }
    }

    pgStream.sendChar('F');
    pgStream.sendInteger4(4 + 4 + 2 + 2 * paramCount + 2 + encodedSize + 2);
    pgStream.sendInteger4(fnid);
    pgStream.sendInteger2(paramCount);
    for (int i = 1; i <= paramCount; ++i) {
      pgStream.sendInteger2(params.isBinary(i) ? 1 : 0);
    }
    pgStream.sendInteger2(paramCount);
    for (int i = 1; i <= paramCount; i++) {
      if (params.isNull(i)) {
        pgStream.sendInteger4(-1);
      } else {
        pgStream.sendInteger4(params.getV3Length(i)); // Parameter size
        params.writeV3Value(i, pgStream);
      }
    }
    pgStream.sendInteger2(1); // Binary result format
    pgStream.flush();
  }
*/  

  // Just for API compatibility with previous versions.
  public synchronized void processNotifies() throws SQLException {
    processNotifies(-1);
  }

  /**
   * @param timeoutMillis when &gt; 0, block for this time
   *                      when =0, block forever
   *                      when &lt; 0, don't block
   */
  public synchronized void processNotifies(int timeoutMillis) throws SQLException {
    waitOnLock();
    // Asynchronous notifies only arrive when we are not in a transaction
    if (getTransactionState() != TransactionState.IDLE) {
      return;
    }

    if (hasNotifications()) {
      // No need to timeout when there are already notifications. We just check for more in this case.
      timeoutMillis = -1;
    }

    boolean useTimeout = timeoutMillis > 0;
    long startTime = 0;
    int oldTimeout = 0;
    if (useTimeout) {
      startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      try {
        oldTimeout = pgStream.getSocket().getSoTimeout();
      } catch (SocketException e) {
        throw new RedshiftException(GT.tr("An error occurred while trying to get the socket "
          + "timeout."), RedshiftState.CONNECTION_FAILURE, e);
      }
    }

    try {
      while (timeoutMillis >= 0 || pgStream.hasMessagePending()) {
        if (useTimeout && timeoutMillis >= 0) {
          setSocketTimeout(timeoutMillis);
        }
        int c = pgStream.receiveChar();
        if (useTimeout && timeoutMillis >= 0) {
          setSocketTimeout(0); // Don't timeout after first char
        }
        switch (c) {
          case 'A': // Asynchronous Notify
            receiveAsyncNotify();
            timeoutMillis = -1;
            continue;
          case 'E':
            // Error Response (response to pretty much everything; backend then skips until Sync)
            throw receiveErrorResponse(false);
          case 'N': // Notice Response (warnings / info)
            SQLWarning warning = receiveNoticeResponse();
            addWarning(warning);
            if (useTimeout) {
              long newTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
              timeoutMillis += startTime - newTimeMillis; // Overflows after 49 days, ignore that
              startTime = newTimeMillis;
              if (timeoutMillis == 0) {
                timeoutMillis = -1; // Don't accidentially wait forever
              }
            }
            break;
          default:
            throw new RedshiftException(GT.tr("Unknown Response Type {0}.", (char) c),
                RedshiftState.CONNECTION_FAILURE);
        }
      }
    } catch (SocketTimeoutException ioe) {
      // No notifications this time...
    } catch (IOException ioe) {
      throw new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
          RedshiftState.CONNECTION_FAILURE, ioe);
    } finally {
      if (useTimeout) {
        setSocketTimeout(oldTimeout);
      }
    }
  }

  private void setSocketTimeout(int millis) throws RedshiftException {
    try {
      Socket s = pgStream.getSocket();
      if (!s.isClosed()) { // Is this check required?
        pgStream.setNetworkTimeout(millis);
      }
    } catch (IOException e) {
      throw new RedshiftException(GT.tr("An error occurred while trying to reset the socket timeout."),
        RedshiftState.CONNECTION_FAILURE, e);
    }
  }

  /* Not in use.
  private byte[] receiveFastpathResult() throws IOException, SQLException {
    boolean endQuery = false;
    SQLException error = null;
    byte[] returnValue = null;

    while (!endQuery) {
      int c = pgStream.receiveChar();
      switch (c) {
        case 'A': // Asynchronous Notify
          receiveAsyncNotify();
          break;

        case 'E':
          // Error Response (response to pretty much everything; backend then skips until Sync)
          SQLException newError = receiveErrorResponse(false);
          if (error == null) {
            error = newError;
          } else {
            error.setNextException(newError);
          }
          // keep processing
          break;

        case 'N': // Notice Response (warnings / info)
          SQLWarning warning = receiveNoticeResponse();
          addWarning(warning);
          break;

        case 'Z': // Ready For Query (eventual response to Sync)
          receiveRFQ();
          endQuery = true;
          break;

        case 'V': // FunctionCallResponse
          int msgLen = pgStream.receiveInteger4();
          int valueLen = pgStream.receiveInteger4();

        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE FunctionCallResponse({0} bytes)", valueLen);

          if (valueLen != -1) {
            byte[] buf = new byte[valueLen];
            pgStream.receive(buf, 0, valueLen);
            returnValue = buf;
          }

          break;

        default:
          throw new RedshiftException(GT.tr("Unknown Response Type {0}.", (char) c),
              RedshiftState.CONNECTION_FAILURE);
      }

    }

    // did we get an error during this query?
    if (error != null) {
      throw error;
    }

    return returnValue;
  }
*/ 

  //
  // Copy subprotocol implementation
  //

  /**
   * Sends given query to BE to start, initialize and lock connection for a CopyOperation.
   *
   * @param sql COPY FROM STDIN / COPY TO STDOUT statement
   * @return CopyIn or CopyOut operation object
   * @throws SQLException on failure
   */
  public  CopyOperation startCopy(String sql, boolean suppressBegin)
      throws SQLException {
  	
  	return copyQueryExecutor.startCopy(sql, suppressBegin);
  }

  /**
   * Finishes a copy operation and unlocks connection discarding any exchanged data.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @throws SQLException on any additional failure
   */
  public void cancelCopy(CopyOperationImpl op) throws SQLException {
  	copyQueryExecutor.cancelCopy(op);
  }

  /**
   * Finishes writing to copy and unlocks connection.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @return number of rows updated for server versions 8.2 or newer
   * @throws SQLException on failure
   */
  public synchronized long endCopy(CopyOperationImpl op) throws SQLException {
  	return copyQueryExecutor.endCopy(op);
  }

  /**
   * Sends data during a live COPY IN operation. Only unlocks the connection if server suddenly
   * returns CommandComplete, which should not happen
   *
   * @param op the CopyIn operation presumably currently holding lock on this connection
   * @param data bytes to send
   * @param off index of first byte to send (usually 0)
   * @param siz number of bytes to send (usually data.length)
   * @throws SQLException on failure
   */
  public synchronized void writeToCopy(CopyOperationImpl op, byte[] data, int off, int siz)
      throws SQLException {
  	copyQueryExecutor.writeToCopy(op, data, off, siz);
  }

  /**
   * Sends data during a live COPY IN operation. Only unlocks the connection if server suddenly
   * returns CommandComplete, which should not happen
   *
   * @param op   the CopyIn operation presumably currently holding lock on this connection
   * @param from the source of bytes, e.g. a ByteBufferByteStreamWriter
   * @throws SQLException on failure
   */
  public synchronized void writeToCopy(CopyOperationImpl op, ByteStreamWriter from)
      throws SQLException {
  	copyQueryExecutor.writeToCopy(op, from);
  }

  public synchronized void flushCopy(CopyOperationImpl op) throws SQLException {
  	copyQueryExecutor.flushCopy(op);
  }

  /**
   * Wait for a row of data to be received from server on an active copy operation
   * Connection gets unlocked by processCopyResults() at end of operation.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @param block whether to block waiting for input
   * @throws SQLException on any failure
   */
  synchronized void readFromCopy(CopyOperationImpl op, boolean block) throws SQLException {
  	copyQueryExecutor.readFromCopy(op, block);
  }

  /*
   * To prevent client/server protocol deadlocks, we try to manage the estimated recv buffer size
   * and force a sync +flush and process results if we think it might be getting too full.
   *
   * See the comments above MAX_BUFFERED_RECV_BYTES's declaration for details.
   */
  private void flushIfDeadlockRisk(Query query, boolean disallowBatching,
      ResultHandler resultHandler,
      BatchResultHandler batchHandler,
      final int flags) throws IOException {
    // Assume all statements need at least this much reply buffer space,
    // plus params
    estimatedReceiveBufferBytes += NODATA_QUERY_RESPONSE_SIZE_BYTES;

    SimpleQuery sq = (SimpleQuery) query;
    if (sq.isStatementDescribed()) {
      /*
       * Estimate the response size of the fields and add it to the expected response size.
       *
       * It's impossible for us to estimate the rowcount. We'll assume one row, as that's the common
       * case for batches and we're leaving plenty of breathing room in this approach. It's still
       * not deadlock-proof though; see pgjdbc github issues #194 and #195.
       */
      int maxResultRowSize = sq.getMaxResultRowSize();
      if (maxResultRowSize >= 0) {
        estimatedReceiveBufferBytes += maxResultRowSize;
      } else {
      	if(RedshiftLogger.isEnable())    	
      		logger.log(LogLevel.DEBUG, "Couldn't estimate result size or result size unbounded, "
            + "disabling batching for this query.");
        disallowBatching = true;
      }
    } else {
      /*
       * We only describe a statement if we're expecting results from it, so it's legal to batch
       * unprepared statements. We'll abort later if we get any uresults from them where none are
       * expected. For now all we can do is hope the user told us the truth and assume that
       * NODATA_QUERY_RESPONSE_SIZE_BYTES is enough to cover it.
       */
    }

    if (disallowBatching || estimatedReceiveBufferBytes >= MAX_BUFFERED_RECV_BYTES) {
    	if(RedshiftLogger.isEnable())    	
    		logger.log(LogLevel.DEBUG, "Forcing Sync, receive buffer full or batching disallowed");
      sendSync(true);
      processResults(resultHandler, flags, 0, (query.getSubqueries() != null), 0);
      estimatedReceiveBufferBytes = 0;
      if (batchHandler != null) {
        batchHandler.secureProgress();
      }
    }

  }

  /*
   * Send a query to the backend.
   */
  private void sendQuery(Query query, V3ParameterList parameters, int maxRows, int fetchSize,
      int flags, ResultHandler resultHandler,
      BatchResultHandler batchHandler) throws IOException, SQLException {
    // Now the query itself.
    Query[] subqueries = query.getSubqueries();
    SimpleParameterList[] subparams = parameters.getSubparams();

    // We know this is deprecated, but still respect it in case anyone's using it.
    // PgJDBC its self no longer does.
    @SuppressWarnings("deprecation")
    boolean disallowBatching = (flags & QueryExecutor.QUERY_DISALLOW_BATCHING) != 0;

    if (subqueries == null) {
      flushIfDeadlockRisk(query, disallowBatching, resultHandler, batchHandler, flags);

      // If we saw errors, don't send anything more.
      if (resultHandler.getException() == null) {
        sendOneQuery((SimpleQuery) query, (SimpleParameterList) parameters, maxRows, fetchSize,
            flags);
      }
    } else {
      for (int i = 0; i < subqueries.length; ++i) {
        final Query subquery = subqueries[i];
        flushIfDeadlockRisk(subquery, disallowBatching, resultHandler, batchHandler, flags);

        // If we saw errors, don't send anything more.
        if (resultHandler.getException() != null) {
          break;
        }

        // In the situation where parameters is already
        // NO_PARAMETERS it cannot know the correct
        // number of array elements to return in the
        // above call to getSubparams(), so it must
        // return null which we check for here.
        //
        SimpleParameterList subparam = SimpleQuery.NO_PARAMETERS;
        if (subparams != null) {
          subparam = subparams[i];
        }
        sendOneQuery((SimpleQuery) subquery, subparam, maxRows, fetchSize, flags);
      }
    }
  }

  //
  // Message sending
  //

  private void sendSync(boolean addInQueue) throws IOException {
  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> Sync");

    pgStream.sendChar('S'); // Sync
    pgStream.sendInteger4(4); // Length
    pgStream.flush();
    
    if (addInQueue) {
	    // Below "add queues" are likely not required at all
	    pendingExecuteQueue.add(new ExecuteRequest(sync, null, true));
	    pendingDescribePortalQueue.add(sync);
    }
  }

  private void sendFlush() throws IOException {
  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> Flush");

    pgStream.sendChar('H'); // Flush
    pgStream.sendInteger4(4); // Length
    pgStream.flush();
    // Below "add queues" are likely not required at all
//    pendingExecuteQueue.add(new ExecuteRequest(sync, null, true));
//    pendingDescribePortalQueue.add(sync);
  }
  
  private void sendParse(SimpleQuery query, SimpleParameterList params, boolean oneShot)
      throws IOException {
    // Already parsed, or we have a Parse pending and the types are right?
    int[] typeOIDs = params.getTypeOIDs();
    if (query.isPreparedFor(typeOIDs, deallocateEpoch)) {
      return;
    }

    // Clean up any existing statement, as we can't use it.
    query.unprepare();
    processDeadParsedQueries();

    // Remove any cached Field values. The re-parsed query might report different
    // fields because input parameter types may result in different type inferences
    // for unspecified types.
    query.setFields(null);

    String statementName = null;
    if (!oneShot) {
      // Generate a statement name to use.
      statementName = "S_" + (nextUniqueID++) + "-" + System.nanoTime();

      // And prepare the new statement.
      // NB: Must clone the OID array, as it's a direct reference to
      // the SimpleParameterList's internal array that might be modified
      // under us.
      query.setStatementName(statementName, deallocateEpoch);
      query.setPrepareTypes(typeOIDs);
      registerParsedQuery(query, statementName);
    }

    byte[] encodedStatementName = query.getEncodedStatementName();
    String nativeSql = query.getNativeSql();

    if (RedshiftLogger.isEnable()) {
      StringBuilder sbuf = new StringBuilder(" FE=> Parse(stmt=" + statementName + ",query=\"");
      sbuf.append(QuerySanitizer.filterCredentials(nativeSql));
      sbuf.append("\",oids={");
      for (int i = 1; i <= params.getParameterCount(); ++i) {
        if (i != 1) {
          sbuf.append(",");
        }
        sbuf.append(params.getTypeOID(i));
      }
      sbuf.append("})");
    	if(RedshiftLogger.isEnable())    	
    		logger.log(LogLevel.DEBUG, sbuf.toString());
    }

    //
    // Send Parse.
    //

    byte[] queryUtf8 = Utils.encodeUTF8(nativeSql);

    // Total size = 4 (size field)
    // + N + 1 (statement name, zero-terminated)
    // + N + 1 (query, zero terminated)
    // + 2 (parameter count) + N * 4 (parameter types)
    int encodedSize = 4
        + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1
        + queryUtf8.length + 1
        + 2 + 4 * params.getParameterCount();

    pgStream.sendChar('P'); // Parse
    pgStream.sendInteger4(encodedSize);
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName);
    }
    pgStream.sendChar(0); // End of statement name
    pgStream.send(queryUtf8); // Query string
    pgStream.sendChar(0); // End of query string.
    pgStream.sendInteger2(params.getParameterCount()); // # of parameter types specified
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      pgStream.sendInteger4(params.getTypeOID(i));
    }

    pendingParseQueue.add(query);
  }

  private void sendBind(SimpleQuery query, SimpleParameterList params, Portal portal,
      boolean noBinaryTransfer) throws IOException {
    //
    // Send Bind.
    //

    String statementName = query.getStatementName();
    byte[] encodedStatementName = query.getEncodedStatementName();
    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());

    if (RedshiftLogger.isEnable()) {
      StringBuilder sbuf = new StringBuilder(" FE=> Bind(stmt=" + statementName + ",portal=" + portal);
      for (int i = 1; i <= params.getParameterCount(); ++i) {
        sbuf.append(",$").append(i).append("=<")
            .append(params.toString(i,true))
            .append(">,type=").append(Oid.toString(params.getTypeOID(i)));
      }
      sbuf.append(")");
    	if(RedshiftLogger.isEnable())    	
    		logger.log(LogLevel.DEBUG, sbuf.toString());
    }

    // Total size = 4 (size field) + N + 1 (destination portal)
    // + N + 1 (statement name)
    // + 2 (param format code count) + N * 2 (format codes)
    // + 2 (param value count) + N (encoded param value size)
    // + 2 (result format code count, 0)
    long encodedSize = 0;
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      if (params.isNull(i)) {
        encodedSize += 4;
      } else {
        encodedSize += (long) 4 + params.getV3Length(i);
      }
    }

    Field[] fields = query.getFields();
    if (!noBinaryTransfer && query.needUpdateFieldFormats()) {
      for (Field field : fields) {
        if (useBinary(field)) {
          field.setFormat(Field.BINARY_FORMAT);
          query.setHasBinaryFields(true);
        }
      }
    }
    // If text-only results are required (e.g. updateable resultset), and the query has binary columns,
    // flip to text format.
    if (noBinaryTransfer && query.hasBinaryFields()) {
      for (Field field : fields) {
        if (field.getFormat() != Field.TEXT_FORMAT) {
          field.setFormat(Field.TEXT_FORMAT);
        }
      }
      query.resetNeedUpdateFieldFormats();
      query.setHasBinaryFields(false);
    }

    // This is not the number of binary fields, but the total number
    // of fields if any of them are binary or zero if all of them
    // are text.
    int numBinaryFields = !noBinaryTransfer && query.hasBinaryFields() ? fields.length : 0;

    encodedSize = 4
        + (encodedPortalName == null ? 0 : encodedPortalName.length) + 1
        + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1
        + 2 + params.getParameterCount() * 2
        + 2 + encodedSize
        + 2 + numBinaryFields * 2;

    // backend's MaxAllocSize is the largest message that can
    // be received from a client. If we have a bigger value
    // from either very large parameters or incorrect length
    // descriptions of setXXXStream we do not send the bind
    // messsage.
    //
    if (encodedSize > 0x3fffffff) {
      throw new RedshiftBindException(new IOException(GT.tr(
          "Bind message length {0} too long.  This can be caused by very large or incorrect length specifications on InputStream parameters.",
          encodedSize)));
    }

    pgStream.sendChar('B'); // Bind
    pgStream.sendInteger4((int) encodedSize); // Message size
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // Destination portal name.
    }
    pgStream.sendChar(0); // End of portal name.
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName); // Source statement name.
    }
    pgStream.sendChar(0); // End of statement name.

    pgStream.sendInteger2(params.getParameterCount()); // # of parameter format codes
    for (int i = 1; i <= params.getParameterCount(); ++i) {
      pgStream.sendInteger2(params.isBinary(i) ? 1 : 0); // Parameter format code
    }

    pgStream.sendInteger2(params.getParameterCount()); // # of parameter values

    // If an error occurs when reading a stream we have to
    // continue pumping out data to match the length we
    // said we would. Once we've done that we throw
    // this exception. Multiple exceptions can occur and
    // it really doesn't matter which one is reported back
    // to the caller.
    //
    RedshiftBindException bindException = null;

    for (int i = 1; i <= params.getParameterCount(); ++i) {
      if (params.isNull(i)) {
        pgStream.sendInteger4(-1); // Magic size of -1 means NULL
      } else {
        pgStream.sendInteger4(params.getV3Length(i)); // Parameter size
        try {
          params.writeV3Value(i, pgStream); // Parameter value
        } catch (RedshiftBindException be) {
          bindException = be;
        }
      }
    }

    pgStream.sendInteger2(numBinaryFields); // # of result format codes
    for (int i = 0; i < numBinaryFields; ++i) {
      pgStream.sendInteger2(fields[i].getFormat());
    }

    pendingBindQueue.add(portal == null ? UNNAMED_PORTAL : portal);

    if (bindException != null) {
      throw bindException;
    }
  }

  /**
   * Returns true if the specified field should be retrieved using binary encoding.
   *
   * @param field The field whose Oid type to analyse.
   * @return True if {@link Field#BINARY_FORMAT} should be used, false if
   *         {@link Field#BINARY_FORMAT}.
   */
  private boolean useBinary(Field field) {
    int oid = field.getOID();
    return useBinaryForReceive(oid);
  }

  private void sendDescribePortal(SimpleQuery query, Portal portal) throws IOException {
    //
    // Send Describe.
    //

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> Describe(portal={0})", portal);

    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());

    // Total size = 4 (size field) + 1 (describe type, 'P') + N + 1 (portal name)
    int encodedSize = 4 + 1 + (encodedPortalName == null ? 0 : encodedPortalName.length) + 1;

    pgStream.sendChar('D'); // Describe
    pgStream.sendInteger4(encodedSize); // message size
    pgStream.sendChar('P'); // Describe (Portal)
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // portal name to close
    }
    pgStream.sendChar(0); // end of portal name

    pendingDescribePortalQueue.add(query);
    query.setPortalDescribed(true);
  }

  private void sendDescribeStatement(SimpleQuery query, SimpleParameterList params,
      boolean describeOnly) throws IOException {
    // Send Statement Describe

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> Describe(statement={0})", query.getStatementName());

    byte[] encodedStatementName = query.getEncodedStatementName();

    // Total size = 4 (size field) + 1 (describe type, 'S') + N + 1 (portal name)
    int encodedSize = 4 + 1 + (encodedStatementName == null ? 0 : encodedStatementName.length) + 1;

    pgStream.sendChar('D'); // Describe
    pgStream.sendInteger4(encodedSize); // Message size
    pgStream.sendChar('S'); // Describe (Statement);
    if (encodedStatementName != null) {
      pgStream.send(encodedStatementName); // Statement name
    }
    pgStream.sendChar(0); // end message

    // Note: statement name can change over time for the same query object
    // Thus we take a snapshot of the query name
    pendingDescribeStatementQueue.add(
        new DescribeRequest(query, params, describeOnly, query.getStatementName()));
    pendingDescribePortalQueue.add(query);
    query.setStatementDescribed(true);
    query.setPortalDescribed(true);
  }

  private void sendExecute(SimpleQuery query, Portal portal, int limit) throws IOException {
    //
    // Send Execute.
    //
    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " FE=> Execute(portal={0},limit={1})", new Object[]{portal, limit});
    }

    byte[] encodedPortalName = (portal == null ? null : portal.getEncodedPortalName());
    int encodedSize = (encodedPortalName == null ? 0 : encodedPortalName.length);

    // Total size = 4 (size field) + 1 + N (source portal) + 4 (max rows)
    pgStream.sendChar('E'); // Execute
    pgStream.sendInteger4(4 + 1 + encodedSize + 4); // message size
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName); // portal name
    }
    pgStream.sendChar(0); // portal name terminator
    pgStream.sendInteger4(limit); // row limit

    pendingExecuteQueue.add(new ExecuteRequest(query, portal, false));
  }

  private void sendClosePortal(String portalName) throws IOException {
    //
    // Send Close.
    //

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> ClosePortal({0})", portalName);

    byte[] encodedPortalName = (portalName == null ? null : Utils.encodeUTF8(portalName));
    int encodedSize = (encodedPortalName == null ? 0 : encodedPortalName.length);

    // Total size = 4 (size field) + 1 (close type, 'P') + 1 + N (portal name)
    pgStream.sendChar('C'); // Close
    pgStream.sendInteger4(4 + 1 + 1 + encodedSize); // message size
    pgStream.sendChar('P'); // Close (Portal)
    if (encodedPortalName != null) {
      pgStream.send(encodedPortalName);
    }
    pgStream.sendChar(0); // unnamed portal
  }

  private void sendCloseStatement(String statementName) throws IOException {
    //
    // Send Close.
    //

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> CloseStatement({0})", statementName);

    byte[] encodedStatementName = (statementName == null)
    																? null
    																: Utils.encodeUTF8(statementName);
    int encodedSize = (encodedStatementName == null ? 0 : encodedStatementName.length);
    

    // Total size = 4 (size field) + 1 (close type, 'S') + N + 1 (statement name)
    pgStream.sendChar('C'); // Close
    pgStream.sendInteger4(4 + 1 + encodedSize + 1); // message size
    pgStream.sendChar('S'); // Close (Statement)
    if (encodedStatementName != null)
    	pgStream.send(encodedStatementName); // statement to close
    pgStream.sendChar(0); // statement name terminator or unnamed statement
  }

  // sendOneQuery sends a single statement via the extended query protocol.
  // Per the FE/BE docs this is essentially the same as how a simple query runs
  // (except that it generates some extra acknowledgement messages, and we
  // can send several queries before doing the Sync)
  //
  // Parse S_n from "query string with parameter placeholders"; skipped if already done previously
  // or if oneshot
  // Bind C_n from S_n plus parameters (or from unnamed statement for oneshot queries)
  // Describe C_n; skipped if caller doesn't want metadata
  // Execute C_n with maxRows limit; maxRows = 1 if caller doesn't want results
  // (above repeats once per call to sendOneQuery)
  // Sync (sent by caller)
  //
  private void sendOneQuery(SimpleQuery query, SimpleParameterList params, int maxRows,
      int fetchSize, int flags) throws IOException {
    boolean asSimple = (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) != 0;
    if (asSimple) {
      assert (flags & QueryExecutor.QUERY_DESCRIBE_ONLY) == 0
          : "Simple mode does not support describe requests. sql = " + query.getNativeSql()
          + ", flags = " + flags;
      sendSimpleQuery(query, params);
      return;
    }

    assert !query.getNativeQuery().multiStatement
        : "Queries that might contain ; must be executed with QueryExecutor.QUERY_EXECUTE_AS_SIMPLE mode. "
        + "Given query is " + query.getNativeSql();

    // Per https://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
    // A Bind message can use the unnamed prepared statement to create a named portal.
    // If the Bind is successful, an Execute message can reference that named portal until either
    //      the end of the current transaction
    //   or the named portal is explicitly destroyed

    boolean noResults = (flags & QueryExecutor.QUERY_NO_RESULTS) != 0;
    boolean noMeta = (flags & QueryExecutor.QUERY_NO_METADATA) != 0;
    boolean describeOnly = (flags & QueryExecutor.QUERY_DESCRIBE_ONLY) != 0;
    boolean oneShot = (flags & QueryExecutor.QUERY_ONESHOT) != 0;
    // extended queries always use a portal
    // the usePortal flag controls whether or not we use a *named* portal
    boolean usePortal = !oneShot ||
    		(
	    		(flags & QueryExecutor.QUERY_FORWARD_CURSOR) != 0 
	    		&& !noResults 
	    		&& !noMeta
	        && fetchSize > 0
	        && !describeOnly
        );
    boolean noBinaryTransfer = (flags & QUERY_NO_BINARY_TRANSFER) != 0;
    boolean forceDescribePortal = (flags & QUERY_FORCE_DESCRIBE_PORTAL) != 0;
    boolean autoCommit = (flags & QueryExecutor.QUERY_SUPPRESS_BEGIN) != 0;

    // Work out how many rows to fetch in this pass.

    int rows;
    if (noResults) {
      rows = 1; // We're discarding any results anyway, so limit data transfer to a minimum
    } else if (!usePortal || autoCommit) {
      rows = maxRows; // Not using a portal or auto-committing -- fetchSize is irrelevant
    } else if (maxRows != 0 && (enableFetchRingBuffer || fetchSize > maxRows)) {
      // fetchSize > maxRows, use maxRows (nb: fetchSize cannot be 0 if usePortal == true)
      rows = maxRows;
    } else {
      rows = (enableFetchRingBuffer) 
      					? maxRows // Disable server cursor, when client cursor is enabled.
      					: fetchSize; // maxRows > fetchSize
    }

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " FE=> OneQuery(rows=\"{0}\")", rows);
    }

    sendParse(query, params, oneShot);

    // Must do this after sendParse to pick up any changes to the
    // query's state.
    //
    boolean queryHasUnknown = query.hasUnresolvedTypes();
    boolean paramsHasUnknown = params.hasUnresolvedTypes();

    boolean describeStatement = describeOnly
        || (!oneShot && paramsHasUnknown && queryHasUnknown && !query.isStatementDescribed());

    if (!describeStatement && paramsHasUnknown && !queryHasUnknown) {
      int[] queryOIDs = query.getPrepareTypes();
      int[] paramOIDs = params.getTypeOIDs();
      for (int i = 0; i < paramOIDs.length; i++) {
        // Only supply type information when there isn't any
        // already, don't arbitrarily overwrite user supplied
        // type information.
        if (paramOIDs[i] == Oid.UNSPECIFIED) {
          params.setResolvedType(i + 1, queryOIDs[i]);
        }
      }
    }

    if (describeStatement) {
      sendDescribeStatement(query, params, describeOnly);
      if (describeOnly) {
        return;
      }
    }

    // Construct a new portal if needed.
    Portal portal = null;
    if (usePortal) 
    {
      String portalName = "C_" + (nextUniqueID++) + "-" + System.nanoTime();
      portal = new Portal(query, portalName);
    }

    sendBind(query, params, portal, noBinaryTransfer);

    // A statement describe will also output a RowDescription,
    // so don't reissue it here if we've already done so.
    //
    if (!noMeta && !describeStatement) {
      /*
       * don't send describe if we already have cached the row description from previous executions
       *
       * XXX Clearing the fields / unpreparing the query (in sendParse) is incorrect, see bug #267.
       * We might clear the cached fields in a later execution of this query if the bind parameter
       * types change, but we're assuming here that they'll still be valid when we come to process
       * the results of this query, so we don't send a new describe here. We re-describe after the
       * fields are cleared, but the result of that gets processed after processing the results from
       * earlier executions that we didn't describe because we didn't think we had to.
       *
       * To work around this, force a Describe at each execution in batches where this can be a
       * problem. It won't cause more round trips so the performance impact is low, and it'll ensure
       * that the field information available when we decoded the results. This is undeniably a
       * hack, but there aren't many good alternatives.
       */
      if (!query.isPortalDescribed() || forceDescribePortal) {
        sendDescribePortal(query, portal);
      }
    }

    sendExecute(query, portal, rows);
  }

  private void sendSimpleQuery(SimpleQuery query, SimpleParameterList params) throws IOException {
    String nativeSql = query.toString(params);

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " FE=> SimpleQuery(query=\"{0}\")", QuerySanitizer.filterCredentials(nativeSql));
    Encoding encoding = pgStream.getEncoding();

    byte[] encoded = encoding.encode(nativeSql);
    pgStream.sendChar('Q');
    pgStream.sendInteger4(encoded.length + 4 + 1);
    pgStream.send(encoded);
    pgStream.sendChar(0);
    pgStream.flush();
    pendingExecuteQueue.add(new ExecuteRequest(query, null, true));
    pendingDescribePortalQueue.add(query);
  }

  //
  // Garbage collection of parsed statements.
  //
  // When a statement is successfully parsed, registerParsedQuery is called.
  // This creates a PhantomReference referring to the "owner" of the statement
  // (the originating Query object) and inserts that reference as a key in
  // parsedQueryMap. The values of parsedQueryMap are the corresponding allocated
  // statement names. The originating Query object also holds a reference to the
  // PhantomReference.
  //
  // When the owning Query object is closed, it enqueues and clears the associated
  // PhantomReference.
  //
  // If the owning Query object becomes unreachable (see java.lang.ref javadoc) before
  // being closed, the corresponding PhantomReference is enqueued on
  // parsedQueryCleanupQueue. In the Sun JVM, phantom references are only enqueued
  // when a GC occurs, so this is not necessarily prompt but should eventually happen.
  //
  // Periodically (currently, just before query execution), the parsedQueryCleanupQueue
  // is polled. For each enqueued PhantomReference we find, we remove the corresponding
  // entry from parsedQueryMap, obtaining the name of the underlying statement in the
  // process. Then we send a message to the backend to deallocate that statement.
  //

  private final HashMap<PhantomReference<SimpleQuery>, String> parsedQueryMap =
      new HashMap<PhantomReference<SimpleQuery>, String>();
  private final ReferenceQueue<SimpleQuery> parsedQueryCleanupQueue =
      new ReferenceQueue<SimpleQuery>();

  private void registerParsedQuery(SimpleQuery query, String statementName) {
    if (statementName == null) {
      return;
    }

    PhantomReference<SimpleQuery> cleanupRef =
        new PhantomReference<SimpleQuery>(query, parsedQueryCleanupQueue);
    parsedQueryMap.put(cleanupRef, statementName);
    query.setCleanupRef(cleanupRef);
  }

  public void closeStatementAndPortal() {
      synchronized(this) {
	    // First, send CloseStatements for finalized SimpleQueries that had statement names assigned.
	    try {
			processDeadParsedQueries();
		    processDeadPortals();
	//	    sendCloseStatement(null);
	//	    sendClosePortal("unnamed");
		    sendFlush();
		    sendSync(false);
		    
		    // Read SYNC response
		    processSyncOnClose();
			} catch (IOException e) {
				// Ignore the error
		    if (RedshiftLogger.isEnable()) {
	    		logger.logError(e);
		    }
			}	catch (SQLException sqe) {
				// Ignore the error
		    if (RedshiftLogger.isEnable()) {
		  		logger.logError(sqe);
		    }
			}
   	} // synchronized
  }
  
  private void processDeadParsedQueries() throws IOException {
    Reference<? extends SimpleQuery> deadQuery;
    while ((deadQuery = parsedQueryCleanupQueue.poll()) != null) {
      String statementName = parsedQueryMap.remove(deadQuery);
      sendCloseStatement(statementName);
      deadQuery.clear();
    }
  }

  //
  // Essentially the same strategy is used for the cleanup of portals.
  // Note that each Portal holds a reference to the corresponding Query
  // that generated it, so the Query won't be collected (and the statement
  // closed) until all the Portals are, too. This is required by the mechanics
  // of the backend protocol: when a statement is closed, all dependent portals
  // are also closed.
  //

  private final HashMap<PhantomReference<Portal>, String> openPortalMap =
      new HashMap<PhantomReference<Portal>, String>();
  private final ReferenceQueue<Portal> openPortalCleanupQueue = new ReferenceQueue<Portal>();

  private static final Portal UNNAMED_PORTAL = new Portal(null, "unnamed");

  private void registerOpenPortal(Portal portal) {
    if (portal == UNNAMED_PORTAL) {
      return; // Using the unnamed portal.
    }

    String portalName = portal.getPortalName();
    PhantomReference<Portal> cleanupRef =
        new PhantomReference<Portal>(portal, openPortalCleanupQueue);
    openPortalMap.put(cleanupRef, portalName);
    portal.setCleanupRef(cleanupRef);
  }

  private void processDeadPortals() throws IOException {
    Reference<? extends Portal> deadPortal;
    while ((deadPortal = openPortalCleanupQueue.poll()) != null) {
      String portalName = openPortalMap.remove(deadPortal);
      sendClosePortal(portalName);
      deadPortal.clear();
    }
  }
  
  /**
   * Check for a running ring buffer thread.
   * 
   * @return returns true if Ring buffer thread is running, otherwise false.
   */
  @Override
  public boolean isRingBufferThreadRunning() {
  	return (m_ringBufferThread != null);
  }
  
  /**
   * Close the last active ring buffer thread.
   */
  @Override
  public void closeRingBufferThread(RedshiftRowsBlockingQueue<Tuple> queueRows, Thread ringBufferThread) {
    // Abort current ring buffer thread, if any.
    waitForRingBufferThreadToFinish(false, true, false, queueRows, ringBufferThread);
  }
  
  @Override
  public void sendQueryCancel() throws SQLException {
  	super.sendQueryCancel();
  }
  
  protected void processSyncOnClose() throws IOException, SQLException {
    int c;
    boolean endQuery = false;
    SQLException error = null;

    while (!endQuery) {
      c = pgStream.receiveChar();

      switch (c) {
        case 'A': // Asynchronous Notify
          receiveAsyncNotify();
          break;
          
        case '3': // Close Complete (response to Close)
          pgStream.receiveInteger4(); // len, discarded
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE CloseComplete");
          break;
          
        case 'E':
          // Error Response (response to pretty much everything; backend then skips until Sync)
          SQLException newError = receiveErrorResponse(true);
          if (error == null) {
            error = newError;
          } else {
            error.setNextException(newError);
          }
          // keep processing
          break;
          
        case 'N': // Notice Response (warnings / info)
          SQLWarning warning = receiveNoticeResponse();
          addWarning(warning);
          break;

        case 'Z': // Ready For Query (eventual response to Sync)
          receiveRFQ();
          pendingExecuteQueue.clear(); // No more query executions expected.
          endQuery = true;
          break;
          
        default:
          throw new IOException("Unexpected packet type: " + c);
      } // switch
    }// while loop
    
    // did we get an error during this query?
    if (error != null) {
      throw error;
    }
  }
  
  protected void processResults(ResultHandler handler, int flags, int fetchSize, boolean subQueries, int maxRows) throws IOException {
  	processResults(handler, flags, fetchSize, subQueries, 0, maxRows);
  }

  protected void processResults(ResultHandler handler, int flags, int fetchSize, boolean subQueries, int initRowCount, int maxRows) throws IOException {
  	MessageLoopState msgLoopState = new MessageLoopState();
  	int[] rowCount = new int[1];
  	rowCount[0] = initRowCount;
		// Process messages on the same application main thread.
		processResultsOnThread(handler, flags, fetchSize, msgLoopState, subQueries, rowCount, maxRows);
  }
  
  private void processResultsOnThread(ResultHandler handler, 
  				int flags, int fetchSize, 
  				MessageLoopState msgLoopState,
  				boolean subQueries,
  				int[] rowCount,
  				int maxRows) throws IOException {
    boolean noResults = (flags & QueryExecutor.QUERY_NO_RESULTS) != 0;
    boolean bothRowsAndStatus = (flags & QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS) != 0;
    boolean useRingBuffer = enableFetchRingBuffer 
    												&& (!handler.wantsScrollableResultSet()) // Scrollable cursor
    												&& (!subQueries) // Multiple results
    												&& (!bothRowsAndStatus); // RETURNING clause 

    List<Tuple> tuples = null;

    int c;
    boolean endQuery = false;
    
    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, "  useRingBuffer={0}, handler.wantsScrollableResultSet()={1}, subQueries={2}, bothRowsAndStatus={3}",
          new Object[]{useRingBuffer, handler.wantsScrollableResultSet(), subQueries, bothRowsAndStatus});
    }
    

    while (!endQuery) {
      c = pgStream.receiveChar();
      switch (c) {
        case 'A': // Asynchronous Notify
          receiveAsyncNotify();
          break;

        case '1': // Parse Complete (response to Parse)
          pgStream.receiveInteger4(); // len, discarded

          SimpleQuery parsedQuery = pendingParseQueue.removeFirst();
          String parsedStatementName = parsedQuery.getStatementName();

        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE ParseComplete [{0}]", parsedStatementName);

          break;

        case 't': { // ParameterDescription
          pgStream.receiveInteger4(); // len, discarded

        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE ParameterDescription");

          DescribeRequest describeData = pendingDescribeStatementQueue.getFirst();
          SimpleQuery query = describeData.query;
          SimpleParameterList params = describeData.parameterList;
          boolean describeOnly = describeData.describeOnly;
          // This might differ from query.getStatementName if the query was re-prepared
          String origStatementName = describeData.statementName;

          int numParams = pgStream.receiveInteger2();

          for (int i = 1; i <= numParams; i++) {
            int typeOid = pgStream.receiveInteger4();
            params.setResolvedType(i, typeOid);
          }

          // Since we can issue multiple Parse and DescribeStatement
          // messages in a single network trip, we need to make
          // sure the describe results we requested are still
          // applicable to the latest parsed query.
          //
          if ((origStatementName == null && query.getStatementName() == null)
              || (origStatementName != null
                  && origStatementName.equals(query.getStatementName()))) {
            query.setPrepareTypes(params.getTypeOIDs());
          }

          if (describeOnly) {
          	msgLoopState.doneAfterRowDescNoData = true;
          } else {
            pendingDescribeStatementQueue.removeFirst();
          }
          break;
        }

        case '2': // Bind Complete (response to Bind)
          pgStream.receiveInteger4(); // len, discarded

          Portal boundPortal = pendingBindQueue.removeFirst();
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE BindComplete [{0}]", boundPortal);

          registerOpenPortal(boundPortal);
          break;

        case '3': // Close Complete (response to Close)
          pgStream.receiveInteger4(); // len, discarded
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE CloseComplete");
          break;

        case 'n': // No Data (response to Describe)
          pgStream.receiveInteger4(); // len, discarded
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE NoData");

          pendingDescribePortalQueue.removeFirst();

          if (msgLoopState.doneAfterRowDescNoData) {
            DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
            SimpleQuery currentQuery = describeData.query;

            Field[] fields = currentQuery.getFields();

            if (fields != null) { // There was a resultset.
            	tuples = new ArrayList<Tuple>();
              handler.handleResultRows(currentQuery, fields, tuples, null, null, rowCount, null);
              tuples = null;
              msgLoopState.queueTuples = null;
            }
          }
          break;

        case 's': { // Portal Suspended (end of Execute)
          // nb: this appears *instead* of CommandStatus.
          // Must be a SELECT if we suspended, so don't worry about it.

          pgStream.receiveInteger4(); // len, discarded
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE PortalSuspended");

          ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
          SimpleQuery currentQuery = executeData.query;
          Portal currentPortal = executeData.portal;

          Field[] fields = currentQuery.getFields();
          if (fields != null 
          			&& (tuples == null 
          						&& msgLoopState.queueTuples == null)) {
            // When no results expected, pretend an empty resultset was returned
            // Not sure if new ArrayList can be always replaced with emptyList
          	tuples = noResults ? Collections.<Tuple>emptyList() : new ArrayList<Tuple>();
          }

          if (msgLoopState.queueTuples != null) {
        		// Mark end of result
        		try {
        			msgLoopState.queueTuples.checkAndAddEndOfRowsIndicator(currentPortal);
						} 
        		catch (InterruptedException ie) {
							// Handle interrupted exception
              handler.handleError(
                  new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
                      RedshiftState.UNEXPECTED_ERROR, ie));
						}
          }
          else
        		handler.handleResultRows(currentQuery, fields, tuples, currentPortal, null, rowCount, null);
          
          tuples = null;
          msgLoopState.queueTuples = null;
          
          break;
        }

        case 'C': { // Command Status (end of Execute)
          // Handle status.
          String status = receiveCommandStatus();
          if (isFlushCacheOnDeallocate()
              && (status.startsWith("DEALLOCATE ALL") || status.startsWith("DISCARD ALL"))) {
            deallocateEpoch++;
          }

          msgLoopState.doneAfterRowDescNoData = false;

          ExecuteRequest executeData = pendingExecuteQueue.peekFirst();
          SimpleQuery currentQuery = executeData.query;
          Portal currentPortal = executeData.portal;

          String nativeSql = currentQuery.getNativeQuery().nativeSql;
          // Certain backend versions (e.g. 12.2, 11.7, 10.12, 9.6.17, 9.5.21, etc)
          // silently rollback the transaction in the response to COMMIT statement
          // in case the transaction has failed.
          // See discussion in pgsql-hackers: https://www.postgresql.org/message-id/b9fb50dc-0f6e-15fb-6555-8ddb86f4aa71%40postgresfriends.org
          if (isRaiseExceptionOnSilentRollback()
              && handler.getException() == null
              && status.startsWith("ROLLBACK")) {
            String message = null;
            if (looksLikeCommit(nativeSql)) {
              if (transactionFailCause == null) {
                message = GT.tr("The database returned ROLLBACK, so the transaction cannot be committed. Transaction failure is not known (check server logs?)");
              } else {
                message = GT.tr("The database returned ROLLBACK, so the transaction cannot be committed. Transaction failure cause is <<{0}>>", transactionFailCause.getMessage());
              }
            } else if (looksLikePrepare(nativeSql)) {
              if (transactionFailCause == null) {
                message = GT.tr("The database returned ROLLBACK, so the transaction cannot be prepared. Transaction failure is not known (check server logs?)");
              } else {
                message = GT.tr("The database returned ROLLBACK, so the transaction cannot be prepared. Transaction failure cause is <<{0}>>", transactionFailCause.getMessage());
              }
            }
            if (message != null) {
              handler.handleError(
                  new RedshiftException(
                      message, RedshiftState.IN_FAILED_SQL_TRANSACTION, transactionFailCause));
            }
          }

          if (status.startsWith("SET")) {
            // Scan only the first 1024 characters to
            // avoid big overhead for long queries.
            if (nativeSql.lastIndexOf("search_path", 1024) != -1
                && !nativeSql.equals(lastSetSearchPathQuery)) {
              // Search path was changed, invalidate prepared statement cache
              lastSetSearchPathQuery = nativeSql;
              deallocateEpoch++;
            }
          }

          if (!executeData.asSimple) {
            pendingExecuteQueue.removeFirst();
          } else {
            // For simple 'Q' queries, executeQueue is cleared via ReadyForQuery message
          }

          // we want to make sure we do not add any results from these queries to the result set
          if (currentQuery == autoSaveQuery
              || currentQuery == releaseAutoSave) {
            // ignore "SAVEPOINT" or RELEASE SAVEPOINT status from autosave query
            break;
          }

          Field[] fields = currentQuery.getFields();
          if (fields != null 
          			&& (tuples == null 
          						&& msgLoopState.queueTuples == null)) {
            // When no results expected, pretend an empty resultset was returned
            // Not sure if new ArrayList can be always replaced with emptyList
          	tuples = noResults ? Collections.<Tuple>emptyList() : new ArrayList<Tuple>();
          }

          // If we received tuples we must know the structure of the
          // resultset, otherwise we won't be able to fetch columns
          // from it, etc, later.
          if (fields == null 
          			&& (tuples != null 
          						|| msgLoopState.queueTuples != null)) {
            throw new IllegalStateException(
                "Received resultset tuples, but no field structure for them");
          }

          if (fields != null 
          			|| (tuples != null 
          						|| msgLoopState.queueTuples != null)) {
            // There was a resultset.
          	if (msgLoopState.queueTuples == null)
          		handler.handleResultRows(currentQuery, fields, tuples, null, null, rowCount, null);
          	else {
          		// Mark end of result
          		try {
          			msgLoopState.queueTuples.checkAndAddEndOfRowsIndicator();
							} catch (InterruptedException ie) {
								// Handle interrupted exception
	              handler.handleError(
	                  new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
	                      RedshiftState.UNEXPECTED_ERROR, ie));
							}
          	}
          	
            tuples = null;
            msgLoopState.queueTuples = null;
            rowCount = new int[1]; // Allocate for the next resultset

            if (bothRowsAndStatus) {
              interpretCommandStatus(status, handler);
            }
          } else {
            interpretCommandStatus(status, handler);
          }

          if (executeData.asSimple) {
            // Simple queries might return several resultsets, thus we clear
            // fields, so queries like "select 1;update; select2" will properly
            // identify that "update" did not return any results
            currentQuery.setFields(null);
          }

          if (currentPortal != null) {
            currentPortal.close();
          }
          break;
        }

        case 'D': // Data Transfer (ongoing Execute response)
        	boolean skipRow = false;
          Tuple tuple = null;
          try {
            tuple = pgStream.receiveTupleV3();
          } catch (OutOfMemoryError oome) {
            if (!noResults) {
              handler.handleError(
                  new RedshiftException(GT.tr("Ran out of memory retrieving query results."),
                      RedshiftState.OUT_OF_MEMORY, oome));
            }
          } catch (SQLException e) {
            handler.handleError(e);
          }
          if (!noResults) {
          	if(rowCount != null) {
        			if(maxRows > 0 && rowCount[0] >= maxRows) {
        				// Ignore any more rows until server fix not to send more rows than max rows.
        				skipRow = true;
        			}
        			else
        				rowCount[0] += 1;
          	}
          	
          	if (useRingBuffer) {
          		boolean firstRow = false;
          		if (msgLoopState.queueTuples == null) {
          			// i.e. First row
          			firstRow = true;
          			msgLoopState.queueTuples = new RedshiftRowsBlockingQueue<Tuple>(fetchSize, fetchRingBufferSize, logger);
          		}
          		
          		// Add row in the queue
          		if(!skipRow) {
	        			try {
	        					msgLoopState.queueTuples.put(tuple);
								} catch (InterruptedException ie) {
										// Handle interrupted exception
			              handler.handleError(
			                  new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
			                      RedshiftState.UNEXPECTED_ERROR, ie));
								}
          		}
        			
        			if(firstRow) {
                // There was a resultset.
                ExecuteRequest executeData = pendingExecuteQueue.peekFirst();
                SimpleQuery currentQuery = executeData.query;
                Field[] fields = currentQuery.getFields();
                
        	  		// Create a new ring buffer thread to process rows
        	  		m_ringBufferThread = new RingBufferThread(handler, flags, fetchSize, msgLoopState, subQueries, rowCount, maxRows);
                
                handler.handleResultRows(currentQuery, fields, null, null, msgLoopState.queueTuples, rowCount, m_ringBufferThread);
                
                if (RedshiftLogger.isEnable()) {
                  int length;
                  if (tuple == null) {
                    length = -1;
                  } else {
                    length = tuple.length();
                  }
                  logger.log(LogLevel.DEBUG, " <=BE DataRow(len={0})", length);
                }
        	  		
        	  		// Start the ring buffer thread
        	  		m_ringBufferThread.start();
        	  		
              	// Return to break the message loop on the application thread
              	return;
        			}
        			else
      				if(m_ringBufferStopThread)
      					return; // Break the ring buffer thread loop
          	}
          	else {
	            if (tuples == null) {
	              tuples = new ArrayList<Tuple>();
	            }
	            
	            if(!skipRow)	            
	            	tuples.add(tuple);
          	}
          }

          if (RedshiftLogger.isEnable()) {
            int length;
            if (tuple == null) {
              length = -1;
            } else {
              length = tuple.length();
            }
          	logger.log(LogLevel.DEBUG, " <=BE DataRow(len={0})", length);
          	if (skipRow) {
          		logger.log(LogLevel.DEBUG, " skipRow={0}, rowCount = {1},  maxRows = {2}"
          					, skipRow, (rowCount!= null) ? rowCount[0] : 0, maxRows);
          	}
          }

          break;

        case 'E':
          // Error Response (response to pretty much everything; backend then skips until Sync)
          SQLException error = receiveErrorResponse(false);
          handler.handleError(error);
          if (willHealViaReparse(error)) {
            // prepared statement ... is not valid kind of error
            // Technically speaking, the error is unexpected, thus we invalidate other
            // server-prepared statements just in case.
            deallocateEpoch++;
            if (RedshiftLogger.isEnable()) {
              logger.log(LogLevel.DEBUG, " FE: received {0}, will invalidate statements. deallocateEpoch is now {1}",
                  new Object[]{error.getSQLState(), deallocateEpoch});
            }
          }
          // keep processing
          break;

        case 'I': { // Empty Query (end of Execute)
          pgStream.receiveInteger4();

        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE EmptyQuery");

          ExecuteRequest executeData = pendingExecuteQueue.removeFirst();
          Portal currentPortal = executeData.portal;
          handler.handleCommandStatus("EMPTY", 0, 0);
          if (currentPortal != null) {
            currentPortal.close();
          }
          break;
        }

        case 'N': // Notice Response
          SQLWarning warning = receiveNoticeResponse();
          handler.handleWarning(warning);
          break;

        case 'S': // Parameter Status
          try {
            receiveParameterStatus();
          } catch (SQLException e) {
            handler.handleError(e);
            endQuery = true;
          }
          break;

        case 'T': // Row Description (response to Describe)
          Field[] fields = receiveFields(serverProtocolVersion);
          tuples = new ArrayList<Tuple>();

          SimpleQuery query = pendingDescribePortalQueue.peekFirst();
          if (!pendingExecuteQueue.isEmpty() && !pendingExecuteQueue.peekFirst().asSimple) {
            pendingDescribePortalQueue.removeFirst();
          }
          query.setFields(fields);

          if (msgLoopState.doneAfterRowDescNoData) {
            DescribeRequest describeData = pendingDescribeStatementQueue.removeFirst();
            SimpleQuery currentQuery = describeData.query;
            currentQuery.setFields(fields);

            if (msgLoopState.queueTuples != null) {
            	// TODO: is this possible?
            }
            
            handler.handleResultRows(currentQuery, fields, tuples, null, null, rowCount, null);
            tuples = null;
            msgLoopState.queueTuples = null;
          }
          break;

        case 'Z': // Ready For Query (eventual response to Sync)
          receiveRFQ();
          if (!pendingExecuteQueue.isEmpty() && pendingExecuteQueue.peekFirst().asSimple) {
          	if (msgLoopState.queueTuples != null) {
          		try {
								msgLoopState.queueTuples.checkAndAddEndOfRowsIndicator();
							} catch (InterruptedException ie) {
								// Handle interrupted exception
	              handler.handleError(
	                  new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
	                      RedshiftState.UNEXPECTED_ERROR, ie));
							}
          	}
            tuples = null;
            msgLoopState.queueTuples = null;
            pgStream.clearResultBufferCount();

            ExecuteRequest executeRequest = pendingExecuteQueue.removeFirst();
            // Simple queries might return several resultsets, thus we clear
            // fields, so queries like "select 1;update; select2" will properly
            // identify that "update" did not return any results
            executeRequest.query.setFields(null);

            pendingDescribePortalQueue.removeFirst();
            if (!pendingExecuteQueue.isEmpty()) {
              if (getTransactionState() == TransactionState.IDLE) {
                handler.secureProgress();
              }
              // process subsequent results (e.g. for cases like batched execution of simple 'Q' queries)
              break;
            }
          }
          endQuery = true;

          // Reset the statement name of Parses that failed.
          while (!pendingParseQueue.isEmpty()) {
            SimpleQuery failedQuery = pendingParseQueue.removeFirst();
            failedQuery.unprepare();
          }

          pendingParseQueue.clear(); // No more ParseComplete messages expected.
          // Pending "describe" requests might be there in case of error
          // If that is the case, reset "described" status, so the statement is properly
          // described on next execution
          while (!pendingDescribeStatementQueue.isEmpty()) {
            DescribeRequest request = pendingDescribeStatementQueue.removeFirst();
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " FE marking setStatementDescribed(false) for query {0}", QuerySanitizer.filterCredentials(request.query.toString()));
            request.query.setStatementDescribed(false);
          }
          while (!pendingDescribePortalQueue.isEmpty()) {
            SimpleQuery describePortalQuery = pendingDescribePortalQueue.removeFirst();
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " FE marking setPortalDescribed(false) for query {0}", QuerySanitizer.filterCredentials(describePortalQuery.toString()));
            describePortalQuery.setPortalDescribed(false);
          }
          pendingBindQueue.clear(); // No more BindComplete messages expected.
          pendingExecuteQueue.clear(); // No more query executions expected.
          break;

        case 'G': // CopyInResponse
        	if(RedshiftLogger.isEnable()) {   	
	          logger.log(LogLevel.DEBUG, " <=BE CopyInResponse");
	          logger.log(LogLevel.DEBUG, " FE=> CopyFail");
        	}

          // COPY sub-protocol is not implemented yet
          // We'll send a CopyFail message for COPY FROM STDIN so that
          // server does not wait for the data.

          byte[] buf = Utils.encodeUTF8(COPY_ERROR_MESSAGE);
          pgStream.sendChar('f');
          pgStream.sendInteger4(buf.length + 4 + 1);
          pgStream.send(buf);
          pgStream.sendChar(0);
          pgStream.flush();
          sendSync(true); // send sync message
          skipMessage(); // skip the response message
          break;

        case 'H': // CopyOutResponse
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE CopyOutResponse");

          skipMessage();
          // In case of CopyOutResponse, we cannot abort data transfer,
          // so just throw an error and ignore CopyData messages
          handler.handleError(
              new RedshiftException(GT.tr(COPY_ERROR_MESSAGE),
                  RedshiftState.NOT_IMPLEMENTED));
          break;

        case 'c': // CopyDone
          skipMessage();
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE CopyDone");
          break;

        case 'd': // CopyData
          skipMessage();
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, " <=BE CopyData");
          break;

        default:
          throw new IOException("Unexpected packet type: " + c);
      }
    }
  }

  /**
   * Ignore the response message by reading the message length and skipping over those bytes in the
   * communication stream.
   */
  void skipMessage() throws IOException {
    int len = pgStream.receiveInteger4();

    assert len >= 4 : "Length from skip message must be at least 4 ";

    // skip len-4 (length includes the 4 bytes for message length itself
    pgStream.skip(len - 4);
  }

  public  void fetch(ResultCursor cursor, ResultHandler handler, int fetchSize, int initRowCount)
      throws SQLException {
    // Wait for current ring buffer thread to finish, if any.
  	// Shouldn't call from synchronized method, which can cause dead-lock.
    waitForRingBufferThreadToFinish(false, false, false, null, null);
  	
    synchronized(this) {
	    waitOnLock();
	    try {
	    	m_executingLock.lock();
		    final Portal portal = (Portal) cursor;
		
		    // Insert a ResultHandler that turns bare command statuses into empty datasets
		    // (if the fetch returns no rows, we see just a CommandStatus..)
		    final ResultHandler delegateHandler = handler;
		    handler = new ResultHandlerDelegate(delegateHandler) {
		      @Override
		      public void handleCommandStatus(String status, long updateCount, long insertOID) {
		        handleResultRows(portal.getQuery(), null, new ArrayList<Tuple>(), null, null, null, null);
		      }
		    };
		
		    // Now actually run it.
		
		    try {
		      processDeadParsedQueries();
		      processDeadPortals();
		
		      sendExecute(portal.getQuery(), portal, fetchSize);
	      	sendFlush();
		      sendSync(true);
		
		      processResults(handler, 0, fetchSize, (portal.getQuery().getSubqueries() != null), initRowCount);
		      estimatedReceiveBufferBytes = 0;
		    } catch (IOException e) {
		      abort();
		      handler.handleError(
		          new RedshiftException(GT.tr("An I/O error occurred while sending to the backend."),
		              RedshiftState.CONNECTION_FAILURE, e));
		    }
		
		    handler.handleCompletion();
	    }
	    finally {
	    	m_executingLock.unlock();	    	
	    }
    } // synchronized
  }

  /*
   * Receive the field descriptions from the back end.
   */
  private Field[] receiveFields(int serverProtocolVersion) throws IOException {
    pgStream.receiveInteger4(); // MESSAGE SIZE
    int size = pgStream.receiveInteger2();
    Field[] fields = new Field[size];

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE RowDescription({0})", size);
    }

    for (int i = 0; i < fields.length; i++) {
      String columnLabel = pgStream.receiveString();
      int tableOid = pgStream.receiveInteger4();
      short positionInTable = (short) pgStream.receiveInteger2();
      int typeOid = pgStream.receiveInteger4();
      int typeLength = pgStream.receiveInteger2();
      int typeModifier = pgStream.receiveInteger4();
      int formatType = pgStream.receiveInteger2();
      fields[i] = new Field(columnLabel,
          typeOid, typeLength, typeModifier, tableOid, positionInTable);
      fields[i].setFormat(formatType);
      
      if (serverProtocolVersion >= ConnectionFactoryImpl.EXTENDED_RESULT_METADATA_SERVER_PROTOCOL_VERSION) {
      	// Read extended resultset metadata
        String schemaName = pgStream.receiveString();
        String tableName = pgStream.receiveString();
        String columnName = pgStream.receiveString();
        String catalogName = pgStream.receiveString();
        int temp = pgStream.receiveInteger2();
      	int nullable = temp & 0x1;
      	int autoincrement = (temp >> 4)  & 0x1;
      	int readOnly = (temp >> 8) & 0x1;
      	int searchable = (temp >> 12) & 0x1;
      	int caseSensitive = 0;

        if (serverProtocolVersion >= ConnectionFactoryImpl.EXTENDED2_RESULT_METADATA_SERVER_PROTOCOL_VERSION) {
        	caseSensitive = (temp >> 1)  & 0x1;
        }
      	
      	fields[i].setMetadata(new FieldMetadata(columnName,
      														tableName, 
      														schemaName,
      														(nullable == 1) ? ResultSetMetaData.columnNoNulls
      																						: ResultSetMetaData.columnNullable,
      														 (autoincrement != 0),
      														 catalogName,
      														 (readOnly != 0),
      														 (searchable != 0),
      														 (caseSensitive != 0)
      														));	
      }

    	if(RedshiftLogger.isEnable())    	
    		logger.log(LogLevel.DEBUG, "        {0}", fields[i]);
    }

    return fields;
  }

  void receiveAsyncNotify() throws IOException {
    int len = pgStream.receiveInteger4(); // MESSAGE SIZE
    assert len > 4 : "Length for AsyncNotify must be at least 4";

    int pid = pgStream.receiveInteger4();
    String msg = pgStream.receiveString();
    String param = pgStream.receiveString();
    addNotification(new com.amazon.redshift.core.Notification(msg, pid, param));

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE AsyncNotify({0},{1},{2})", new Object[]{pid, msg, param});
    }
  }

  SQLException receiveErrorResponse(boolean calledFromClose) throws IOException {
    // it's possible to get more than one error message for a query
    // see libpq comments wrt backend closing a connection
    // so, append messages to a string buffer and keep processing
    // check at the bottom to see if we need to throw an exception

    int elen = pgStream.receiveInteger4();
    assert elen > 4 : "Error response length must be greater than 4";

    EncodingPredictor.DecodeResult totalMessage = pgStream.receiveErrorString(elen - 4);
    ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage);

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE ErrorMessage({0})", errorMsg.toString());
    }

    RedshiftException error = new RedshiftException(errorMsg, this.logServerErrorDetail);
    
    if(!calledFromClose) {
	    if (transactionFailCause == null) {
	      transactionFailCause = error;
	    } else {
	      error.initCause(transactionFailCause);
	    }
    }
    return error;
  }

  SQLWarning receiveNoticeResponse() throws IOException {
    int nlen = pgStream.receiveInteger4();
    assert nlen > 4 : "Notice Response length must be greater than 4";

    ServerErrorMessage warnMsg = new ServerErrorMessage(pgStream.receiveString(nlen - 4));

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE NoticeResponse({0})", warnMsg.toString());
    }

    return new RedshiftWarning(warnMsg);
  }

  String receiveCommandStatus() throws IOException {
    // TODO: better handle the msg len
    int len = pgStream.receiveInteger4();
    // read len -5 bytes (-4 for len and -1 for trailing \0)
    String status = pgStream.receiveString(len - 5);
    // now read and discard the trailing \0
    pgStream.receiveChar(); // Receive(1) would allocate new byte[1], so avoid it

  	if(RedshiftLogger.isEnable())    	
  		logger.log(LogLevel.DEBUG, " <=BE CommandStatus({0})", status);

    return status;
  }

  private void interpretCommandStatus(String status, ResultHandler handler) {
    try {
      commandCompleteParser.parse(status);
    } catch (SQLException e) {
      handler.handleError(e);
      return;
    }
    long oid = commandCompleteParser.getOid();
    long count = commandCompleteParser.getRows();

    handler.handleCommandStatus(status, count, oid);
  }

  void receiveRFQ() throws IOException {
    if (pgStream.receiveInteger4() != 5) {
      throw new IOException("unexpected length of ReadyForQuery message");
    }

    char tStatus = (char) pgStream.receiveChar();
    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE ReadyForQuery({0})", tStatus);
    }

    // Update connection state.
    switch (tStatus) {
      case 'I':
        transactionFailCause = null;
        setTransactionState(TransactionState.IDLE);
        break;
      case 'T':
        transactionFailCause = null;
        setTransactionState(TransactionState.OPEN);
        break;
      case 'E':
        setTransactionState(TransactionState.FAILED);
        break;
      default:
        throw new IOException(
            "unexpected transaction state in ReadyForQuery message: " + (int) tStatus);
    }
  }

  @Override
  protected void sendCloseMessage() throws IOException {
    // Wait for current ring buffer thread to finish, if any.
    waitForRingBufferThreadToFinish(true, false, false, null, null);
  	
    pgStream.sendChar('X');
    pgStream.sendInteger4(4);
  }

  public void readStartupMessages() throws IOException, SQLException {
    for (int i = 0; i < 1000; i++) {
      int beresp = pgStream.receiveChar();
      switch (beresp) {
        case 'Z':
          receiveRFQ();
          // Ready For Query; we're done.
          return;

        case 'K':
          // BackendKeyData
          int msgLen = pgStream.receiveInteger4();
          if (msgLen != 12) {
            throw new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
                RedshiftState.PROTOCOL_VIOLATION);
          }

          int pid = pgStream.receiveInteger4();
          int ckey = pgStream.receiveInteger4();

          if (RedshiftLogger.isEnable()) {
            logger.log(LogLevel.DEBUG, " <=BE BackendKeyData(pid={0},ckey={1})", new Object[]{pid, ckey});
          }

          setBackendKeyData(pid, ckey);
          break;

        case 'E':
          // Error
          throw receiveErrorResponse(false);

        case 'N':
          // Warning
          addWarning(receiveNoticeResponse());
          break;

        case 'S':
          // ParameterStatus
          receiveParameterStatus();

          break;

        default:
          if (RedshiftLogger.isEnable()) {
            logger.log(LogLevel.DEBUG, "  invalid message type={0}", (char) beresp);
          }
          throw new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
              RedshiftState.PROTOCOL_VIOLATION);
      }
    }
    throw new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
        RedshiftState.PROTOCOL_VIOLATION);
  }

  public void receiveParameterStatus() throws IOException, SQLException {
    // ParameterStatus
    pgStream.receiveInteger4(); // MESSAGE SIZE
    String name = pgStream.receiveString();
    String value = pgStream.receiveString();

    if (RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, " <=BE ParameterStatus({0} = {1})", new Object[]{name, value});
    }

    /* Update client-visible parameter status map for getParameterStatuses() */
    if (name != null && !name.equals("")) {
      onParameterStatus(name, value);
    }

    if (name.equals("client_encoding")) {
      if (allowEncodingChanges) {
        if (!value.equalsIgnoreCase("UTF8") && !value.equalsIgnoreCase("UTF-8")) {
        	if(RedshiftLogger.isEnable())    	
	          logger.log(LogLevel.DEBUG,
	              "Redshift jdbc expects client_encoding to be UTF8 for proper operation. Actual encoding is {0}",
	              value);
        }
        pgStream.setEncoding(Encoding.getDatabaseEncoding(value, logger));
      } else if (!value.equalsIgnoreCase("UTF8") && !value.equalsIgnoreCase("UTF-8")) {
        close(); // we're screwed now; we can't trust any subsequent string.
        throw new RedshiftException(GT.tr(
            "The server''s client_encoding parameter was changed to {0}. The JDBC driver requires client_encoding to be UTF8 for correct operation.",
            value), RedshiftState.CONNECTION_FAILURE);

      }
    }

    if (name.equals("DateStyle") && !value.startsWith("ISO")
        && !value.toUpperCase().startsWith("ISO")) {
      close(); // we're screwed now; we can't trust any subsequent date.
      throw new RedshiftException(GT.tr(
          "The server''s DateStyle parameter was changed to {0}. The JDBC driver requires DateStyle to begin with ISO for correct operation.",
          value), RedshiftState.CONNECTION_FAILURE);
    }

    if (name.equals("standard_conforming_strings")) {
      if (value.equals("on")) {
        setStandardConformingStrings(true);
      } else if (value.equals("off")) {
        setStandardConformingStrings(false);
      } else {
        close();
        // we're screwed now; we don't know how to escape string literals
        throw new RedshiftException(GT.tr(
            "The server''s standard_conforming_strings parameter was reported as {0}. The JDBC driver expected on or off.",
            value), RedshiftState.CONNECTION_FAILURE);
      }
      return;
    }

    if ("TimeZone".equals(name)) {
      setTimeZone(TimestampUtils.parseBackendTimeZone(value));
    } else if ("application_name".equals(name)) {
      setApplicationName(value);
    } else if ("server_version_num".equals(name)) {
      setServerVersionNum(Integer.parseInt(value));
    } else if ("server_version".equals(name)) {
      setServerVersion(value);
    } else if ("server_protocol_version".equals(name)) {
      setServerProtocolVersion(value);
    }  else if ("integer_datetimes".equals(name)) {
      if ("on".equals(value)) {
        setIntegerDateTimes(true);
      } else if ("off".equals(value)) {
        setIntegerDateTimes(false);
      } else {
        throw new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
            RedshiftState.PROTOCOL_VIOLATION);
      }
    } 
    else if ("datashare_enabled".equals(name)) {
      if ("on".equals(value)) {
      	setDatashareEnabled(true);
      } 
      else if ("off".equals(value)) {
      	setDatashareEnabled(false);
      } 
      else {
        throw new RedshiftException(GT.tr("Protocol error.  Session setup failed. Invalid value of datashare_enabled parameter. Only on/off are valid values"),
            RedshiftState.PROTOCOL_VIOLATION);
      }
    }
    else if ("external_database".equals(name)) {
        if ("on".equals(value)) {
            setCrossDatasharingEnabled(true);
        }
        else if ("off".equals(value)) {
            setCrossDatasharingEnabled(false);
        }
        else {
            throw new RedshiftException(GT.tr("Protocol error.  Session setup failed. Invalid value of external_database parameter. Only on/off are valid values"),
                    RedshiftState.PROTOCOL_VIOLATION);
        }
    } // enable_redshift_federation
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getApplicationName() {
    if (applicationName == null) {
      return "";
    }
    return applicationName;
  }

  @Override
  public ReplicationProtocol getReplicationProtocol() {
    return replicationProtocol;
  }

  @Override
  public boolean useBinaryForReceive(int oid) {
    return useBinaryReceiveForOids.contains(oid);
  }

  @Override
  public void setBinaryReceiveOids(Set<Integer> oids) {
    useBinaryReceiveForOids.clear();
    useBinaryReceiveForOids.addAll(oids);
  }

  @Override
  public boolean useBinaryForSend(int oid) {
    return useBinarySendForOids.contains(oid);
  }

  @Override
  public void setBinarySendOids(Set<Integer> oids) {
    useBinarySendForOids.clear();
    useBinarySendForOids.addAll(oids);
  }

  private void setIntegerDateTimes(boolean state) {
    integerDateTimes = state;
  }

  public boolean getIntegerDateTimes() {
    return integerDateTimes;
  }
  
  /**
   * Wait for ring buffer thread to finish.
   * 
   * @param calledFromConnectionClose true, if it called from connection.close(), false otherwise.
   * @param calledFromResultsetClose true, if it called from resultset.close(), false otherwise.
   * @param queueRows the blocking queue rows
   * @param ringBufferThread the thread manage the blocking queue
   */
  public void waitForRingBufferThreadToFinish(boolean calledFromConnectionClose, 
  																						boolean calledFromResultsetClose,
  																						boolean calledFromStatementClose,
                                                                                        RedshiftRowsBlockingQueue<Tuple> queueRows,
  																						Thread ringBufferThread)
  {
  	synchronized(m_ringBufferThreadLock) {
  		try {
	  		m_executingLock.lock();
				// Wait for full read of any executing command
				if(m_ringBufferThread != null)
				{
					try
					{
						if(calledFromConnectionClose)
						{
							// Interrupt the current thread
							m_ringBufferStopThread = true;
							m_ringBufferThread.interrupt();
							return;
						}
						else
						if (calledFromResultsetClose)
						{
							// Drain results from the socket 
							if (queueRows != null)
								queueRows.setSkipRows();
							
							// Wait for thread associated with result to terminate.
							if (ringBufferThread != null) {
								ringBufferThread.join();
							}
							
							if (queueRows != null)
								queueRows.close();
						}
                        else if(calledFromStatementClose)
                        {
                            // Drain results from the socket
                            if (queueRows != null)
                                queueRows.setSkipRows();

                            m_ringBufferThread.join();
                        }
						else {
							// Application is trying to execute another SQL on same connection.
							// Wait for current thread to terminate.
							m_ringBufferThread.join(); // joinWaitTime
						}
					}
					catch(Throwable th)
					{
						// Ignore it
					}
				}
				else {
					// Buffer thread is terminated.
					if (queueRows != null && calledFromResultsetClose)
						queueRows.close();
				}
  		}
  		finally {
	  		m_executingLock.unlock();
  		}
  	}
  }
  
  private final Deque<SimpleQuery> pendingParseQueue = new ArrayDeque<SimpleQuery>();
  private final Deque<Portal> pendingBindQueue = new ArrayDeque<Portal>();
  private final Deque<ExecuteRequest> pendingExecuteQueue = new ArrayDeque<ExecuteRequest>();
  private final Deque<DescribeRequest> pendingDescribeStatementQueue =
      new ArrayDeque<DescribeRequest>();
  private final Deque<SimpleQuery> pendingDescribePortalQueue = new ArrayDeque<SimpleQuery>();

  private long nextUniqueID = 1;
  private final boolean allowEncodingChanges;
  private final boolean cleanupSavePoints;

  /**
   * <p>The estimated server response size since we last consumed the input stream from the server, in
   * bytes.</p>
   *
   * <p>Starts at zero, reset by every Sync message. Mainly used for batches.</p>
   *
   * <p>Used to avoid deadlocks, see MAX_BUFFERED_RECV_BYTES.</p>
   */
  private int estimatedReceiveBufferBytes = 0;

  private final SimpleQuery beginTransactionQuery =
      new SimpleQuery(
          new NativeQuery("BEGIN", new int[0], false, SqlCommand.BLANK),
          null, false, logger);

  private final SimpleQuery beginReadOnlyTransactionQuery =
      new SimpleQuery(
          new NativeQuery("BEGIN READ ONLY", new int[0], false, SqlCommand.BLANK),
          null, false, logger);

  private final SimpleQuery emptyQuery =
      new SimpleQuery(
          new NativeQuery("", new int[0], false,
              SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK)
          ), null, false, logger);

  private final SimpleQuery autoSaveQuery =
      new SimpleQuery(
          new NativeQuery("SAVEPOINT RSJDBC_AUTOSAVE", new int[0], false, SqlCommand.BLANK),
          null, false, logger);

  private final SimpleQuery releaseAutoSave =
      new SimpleQuery(
          new NativeQuery("RELEASE SAVEPOINT RSJDBC_AUTOSAVE", new int[0], false, SqlCommand.BLANK),
          null, false, logger);

  /*
  In autosave mode we use this query to roll back errored transactions
   */
  private final SimpleQuery restoreToAutoSave =
      new SimpleQuery(
          new NativeQuery("ROLLBACK TO SAVEPOINT RSJDBC_AUTOSAVE", new int[0], false, SqlCommand.BLANK),
          null, false, logger);
  
  /**
   * Ring Buffer thread to call message loop.
   * It's an inner class because it can access outer class member vars.
   * 
   * @author igarish
   *
   */
  private class RingBufferThread extends Thread
  {
  	// Message loop state
  	ResultHandler handler;
  	int flags;
  	int fetchSize;
  	MessageLoopState msgLoopState;
  	boolean subQueries;
  	int[] rowCount;
  	int maxRows;
  	
  	/**
  	 * Constructor
  	 * 
  	 * @param msgLoopState
  	 */
  	public RingBufferThread(ResultHandler handler, 
  					int flags, int fetchSize, 
  					MessageLoopState msgLoopState,
  					boolean subQueries,
  					int[] rowCount,
  					int maxRows)
  	{
  		super("RingBufferThread");
  		this.handler = handler;
  		this.flags = flags;
  		this.fetchSize = fetchSize;
  		this.msgLoopState = msgLoopState;
  		this.subQueries = subQueries;
  		this.rowCount = rowCount;
  		this.maxRows = maxRows;
  	}
  	
  	/**
  	 * Run the thread
  	 */
  	@Override
  	public void run() 
  	{
  		// TODO: Do we have to synchronize on this?  		
  		try
  		{
  			// Process result
  			processResultsOnThread(handler, flags, fetchSize, msgLoopState, subQueries, rowCount, maxRows);
  		}
  		catch(Exception ex) 
  		{
  			if(m_ringBufferStopThread) {
					// Clear the interrupted flag
					Thread.currentThread().interrupted();
					
	  			// Close the queue
	  			if (this.msgLoopState.queueTuples != null)
	  				this.msgLoopState.queueTuples.close();
  			}
  			else {
  				// Add end-of-result marker
  				if (this.msgLoopState.queueTuples != null) {
  					try {
							this.msgLoopState.queueTuples.checkAndAddEndOfRowsIndicator();
						} catch (Exception e) {
							// Ignore
						}
  				}
  				
					// Handle  exception
	        handler.handleError(
	            new RedshiftException(GT.tr("Exception retrieving query results."),
	                RedshiftState.UNEXPECTED_ERROR, ex));
  			}
  		}
  		finally
  		{
				// Add end-of-result marker
				if (this.msgLoopState.queueTuples != null) {
					try {
						this.msgLoopState.queueTuples.setHandlerException(handler.getException());
						this.msgLoopState.queueTuples.checkAndAddEndOfRowsIndicator();
					} catch (Exception e) {
						// Ignore
					}
				}
  			
				handler.setStatementStateIdleFromInQuery();
				
				// Reset vars
  			this.msgLoopState.queueTuples = null;
  			this.msgLoopState = null;
  			this.handler = null;
  			m_ringBufferStopThread = false;
  			m_ringBufferThread = null;  			
  		}
  	}
  } // RingBufferThread
}
