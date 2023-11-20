/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.RedshiftNotification;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.jdbc.AutoSave;
import com.amazon.redshift.jdbc.EscapeSyntaxCallMode;
import com.amazon.redshift.jdbc.PreferQueryMode;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.HostSpec;
import com.amazon.redshift.util.LruCache;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.ServerErrorMessage;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public abstract class QueryExecutorBase implements QueryExecutor {

  protected RedshiftLogger logger;
  protected final RedshiftStream pgStream;
  private final String user;
  private final String database;
  private final int cancelSignalTimeout;

  private int cancelPid;
  private int cancelKey;
  private boolean closed = false;
  private String serverVersion;
  private int serverVersionNum = 0;
  private TransactionState transactionState;
  private final boolean reWriteBatchedInserts;
  private final boolean columnSanitiserDisabled;
  private final EscapeSyntaxCallMode escapeSyntaxCallMode;
  private final PreferQueryMode preferQueryMode;
  private AutoSave autoSave;
  private boolean flushCacheOnDeallocate = true;
  protected final boolean logServerErrorDetail;
  private boolean raiseExceptionOnSilentRollback;

  // default value for server versions that don't report standard_conforming_strings
  private boolean standardConformingStrings = false;

  private SQLWarning warnings;
  private final ArrayList<RedshiftNotification> notifications = new ArrayList<RedshiftNotification>();

  private final LruCache<Object, CachedQuery> statementCache;
  private final CachedQueryCreateAction cachedQueryCreateAction;

  // For getParameterStatuses(), GUC_REPORT tracking
  private final TreeMap<String,String> parameterStatuses
      = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
  
  protected boolean enableStatementCache;
  protected int serverProtocolVersion;
  protected boolean datashareEnabled;
  protected boolean enableMultiSqlSupport;
  protected boolean isCrossDatasharingEnabled;

  protected Properties properties;


  protected QueryExecutorBase(RedshiftStream pgStream, String user,
      String database, int cancelSignalTimeout, Properties info,
      RedshiftLogger logger) throws SQLException {
  	this.logger = logger;
    this.pgStream = pgStream;
    this.user = user;
    this.database = database;
    this.properties = info;
    this.cancelSignalTimeout = cancelSignalTimeout;
    this.reWriteBatchedInserts = RedshiftProperty.REWRITE_BATCHED_INSERTS.getBoolean(info);
    this.columnSanitiserDisabled = RedshiftProperty.DISABLE_COLUMN_SANITISER.getBoolean(info);
    String callMode = RedshiftProperty.ESCAPE_SYNTAX_CALL_MODE.get(info);
    this.escapeSyntaxCallMode = EscapeSyntaxCallMode.of(callMode);
    String preferMode = RedshiftProperty.PREFER_QUERY_MODE.get(info);
    this.preferQueryMode = PreferQueryMode.of(preferMode);
    this.autoSave = AutoSave.of(RedshiftProperty.AUTOSAVE.get(info));
    this.logServerErrorDetail = RedshiftProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(info);
    this.cachedQueryCreateAction = new CachedQueryCreateAction(this);
    statementCache = new LruCache<Object, CachedQuery>(
        Math.max(0, RedshiftProperty.PREPARED_STATEMENT_CACHE_QUERIES.getInt(info)),
        Math.max(0, RedshiftProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.getInt(info) * 1024 * 1024),
        false,
        cachedQueryCreateAction,
        new LruCache.EvictAction<CachedQuery>() {
          @Override
          public void evict(CachedQuery cachedQuery) throws SQLException {
            cachedQuery.query.close();
          }
        });
    this.datashareEnabled = false;
    this.enableMultiSqlSupport = RedshiftProperty.ENABLE_MULTI_SQL_SUPPORT.getBoolean(info);
  }

  protected abstract void sendCloseMessage() throws IOException;

  @Override
  public void setNetworkTimeout(int milliseconds) throws IOException {
    pgStream.setNetworkTimeout(milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws IOException {
    return pgStream.getNetworkTimeout();
  }

  @Override
  public HostSpec getHostSpec() {
    return pgStream.getHostSpec();
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  public void setBackendKeyData(int cancelPid, int cancelKey) {
    this.cancelPid = cancelPid;
    this.cancelKey = cancelKey;
  }

  @Override
  public int getBackendPID() {
    return cancelPid;
  }

  @Override
  public boolean isCrossDatasharingEnabled() {
    return isCrossDatasharingEnabled;
  }

  @Override
  public void abort() {
    try {
      pgStream.getSocket().close();
    } catch (IOException e) {
      // ignore
    }
    closed = true;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    try {
    	if(RedshiftLogger.isEnable())
    		logger.log(LogLevel.DEBUG, " FE=> Terminate");
      sendCloseMessage();
      pgStream.flush();
      pgStream.close();
    } catch (IOException ioe) {
    	if(RedshiftLogger.isEnable())
    		logger.log(LogLevel.DEBUG, "Discarding IOException on close:", ioe);
    }

    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void sendQueryCancel() throws SQLException {
    if (cancelPid <= 0) {
    	if(RedshiftLogger.isEnable()) {
        logger.logError("sendQueryCancel: cancelPid <= 0 (pid={0})", cancelPid);
      }
      return;
    }

    RedshiftStream cancelStream = null;

    // Now we need to construct and send a cancel packet
    try {
    	if(RedshiftLogger.isEnable()) {
        logger.logDebug(" FE=> CancelRequest(pid={0},ckey={1})", new Object[]{cancelPid, cancelKey});
      }

      cancelStream =
          new RedshiftStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), cancelSignalTimeout, logger, false, this.properties);
      if (cancelSignalTimeout > 0) {
        cancelStream.setNetworkTimeout(cancelSignalTimeout);
      }
      cancelStream.sendInteger4(16);
      cancelStream.sendInteger2(1234);
      cancelStream.sendInteger2(5678);
      cancelStream.sendInteger4(cancelPid);
      cancelStream.sendInteger4(cancelKey);
      cancelStream.flush();
      cancelStream.receiveEOF();
    } catch (IOException e) {
      // Safe to ignore.
    	if(RedshiftLogger.isEnable()) {
    		logger.log(LogLevel.DEBUG, "Ignoring exception on cancel request:", e);
    		logger.logError(e);
    	}
    } finally {
      if (cancelStream != null) {
        try {
          cancelStream.close();
        } catch (IOException e) {
          // Ignored.
        }
      }
    }
  }

  public synchronized void addWarning(SQLWarning newWarning) {
    if (warnings == null) {
      warnings = newWarning;
    } else {
      warnings.setNextWarning(newWarning);
    }
  }

  public void setCrossDatasharingEnabled(boolean isCrossDatasharingEnabled) {
    this.isCrossDatasharingEnabled = isCrossDatasharingEnabled;
  }

  public synchronized void addNotification(RedshiftNotification notification) {
    notifications.add(notification);
  }

  @Override
  public synchronized RedshiftNotification[] getNotifications() throws SQLException {
    RedshiftNotification[] array = notifications.toArray(new RedshiftNotification[0]);
    notifications.clear();
    return array;
  }

  @Override
  public synchronized SQLWarning getWarnings() {
    SQLWarning chain = warnings;
    warnings = null;
    return chain;
  }

  @Override
  public String getServerVersion() {
    return serverVersion;
  }

  @Override
  public int getServerProtocolVersion() {
    return serverProtocolVersion;
  }

  @Override
  public boolean isDatashareEnabled() {
    return datashareEnabled;
  }
  
  @Override
  public int getServerVersionNum() {
    if (serverVersionNum != 0) {
      return serverVersionNum;
    }
    return serverVersionNum = Utils.parseServerVersionStr(serverVersion);
  }

  public void setServerVersion(String serverVersion) {
    this.serverVersion = serverVersion;
  }

  protected void setServerProtocolVersion(String serverProtocolVersion) {
    this.serverProtocolVersion = (serverProtocolVersion != null && serverProtocolVersion.length() != 0) 
    																? Integer.parseInt(serverProtocolVersion) 
    																: 0;
  }

  public void setDatashareEnabled(boolean datashareEnabled) {
    this.datashareEnabled = datashareEnabled;
  }
  
  public void setServerVersionNum(int serverVersionNum) {
    this.serverVersionNum = serverVersionNum;
  }

  public synchronized void setTransactionState(TransactionState state) {
    transactionState = state;
  }

  public synchronized void setStandardConformingStrings(boolean value) {
    standardConformingStrings = value;
  }

  @Override
  public synchronized boolean getStandardConformingStrings() {
    return standardConformingStrings;
  }

  @Override
  public synchronized TransactionState getTransactionState() {
    return transactionState;
  }

  public void setEncoding(Encoding encoding) throws IOException {
    pgStream.setEncoding(encoding);
  }

  @Override
  public Encoding getEncoding() {
    return pgStream.getEncoding();
  }

  @Override
  public boolean isReWriteBatchedInsertsEnabled() {
    return this.reWriteBatchedInserts;
  }
  
  @Override
  public  boolean isMultiSqlSupport() {
    return enableMultiSqlSupport;
  }
  

  @Override
  public final CachedQuery borrowQuery(String sql) throws SQLException {
    return statementCache.borrow(sql);
  }

  @Override
  public final CachedQuery borrowCallableQuery(String sql) throws SQLException {
    return statementCache.borrow(new CallableQueryKey(sql));
  }

  @Override
  public final CachedQuery borrowReturningQuery(String sql, String[] columnNames) throws SQLException {
    return statementCache.borrow(new QueryWithReturningColumnsKey(sql, true, true,
        columnNames
    ));
  }

  @Override
  public CachedQuery borrowQueryByKey(Object key) throws SQLException {
    return statementCache.borrow(key);
  }

  @Override
  public void releaseQuery(CachedQuery cachedQuery) {
  	
  	if(enableStatementCache)
  		statementCache.put(cachedQuery.key, cachedQuery);
  	else {
    	// Disabled the statement cache.
    	// So the driver is not putting the cachedquery in the LRUCache.
    	// Enabled statement cache causes the issue when objects used in same statement, drop/create,
    	// it can change the OID. Then parsed statement is no more useful.
	  	if(cachedQuery != null
	  			&& cachedQuery.query != null) {
	  		cachedQuery.query.close();
	  	}
  	}
  }

  @Override
  public final Object createQueryKey(String sql, boolean escapeProcessing,
      boolean isParameterized, String... columnNames) {
    Object key;
    if (columnNames == null || columnNames.length != 0) {
      // Null means "return whatever sensible columns are" (e.g. primary key, or serial, or something like that)
      key = new QueryWithReturningColumnsKey(sql, isParameterized, escapeProcessing, columnNames);
    } else if (isParameterized) {
      // If no generated columns requested, just use the SQL as a cache key
      key = sql;
    } else {
      key = new BaseQueryKey(sql, false, escapeProcessing);
    }
    return key;
  }

  @Override
  public CachedQuery createQueryByKey(Object key) throws SQLException {
    return cachedQueryCreateAction.create(key);
  }

  @Override
  public final CachedQuery createQuery(String sql, boolean escapeProcessing,
      boolean isParameterized, String... columnNames)
      throws SQLException {
    Object key = createQueryKey(sql, escapeProcessing, isParameterized, columnNames);
    // Note: cache is not reused here for two reasons:
    //   1) Simplify initial implementation for simple statements
    //   2) Non-prepared statements are likely to have literals, thus query reuse would not be often
    return createQueryByKey(key);
  }

  @Override
  public boolean isColumnSanitiserDisabled() {
    return columnSanitiserDisabled;
  }

  @Override
  public EscapeSyntaxCallMode getEscapeSyntaxCallMode() {
    return escapeSyntaxCallMode;
  }

  @Override
  public PreferQueryMode getPreferQueryMode() {
    return preferQueryMode;
  }

  public AutoSave getAutoSave() {
    return autoSave;
  }

  public void setAutoSave(AutoSave autoSave) {
    this.autoSave = autoSave;
  }

  protected boolean willHealViaReparse(SQLException e) {
    if (e == null || e.getSQLState() == null) {
      return false;
    }

    // "prepared statement \"S_2\" does not exist"
    if (RedshiftState.INVALID_SQL_STATEMENT_NAME.getState().equals(e.getSQLState())) {
      return true;
    }
    if (!RedshiftState.NOT_IMPLEMENTED.getState().equals(e.getSQLState())) {
      return false;
    }

    if (!(e instanceof RedshiftException)) {
      return false;
    }

    RedshiftException pe = (RedshiftException) e;

    ServerErrorMessage serverErrorMessage = pe.getServerErrorMessage();
    if (serverErrorMessage == null) {
      return false;
    }
    // "cached plan must not change result type"
    String routine = pe.getServerErrorMessage().getRoutine();
    return "RevalidateCachedQuery".equals(routine) // 9.2+
        || "RevalidateCachedPlan".equals(routine); // <= 9.1
  }

  @Override
  public boolean willHealOnRetry(SQLException e) {
    if (autoSave == AutoSave.NEVER && getTransactionState() == TransactionState.FAILED) {
      // If autorollback is not activated, then every statement will fail with
      // 'transaction is aborted', etc, etc
      return false;
    }
    return willHealViaReparse(e);
  }

  public boolean isFlushCacheOnDeallocate() {
    return flushCacheOnDeallocate;
  }

  public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
    this.flushCacheOnDeallocate = flushCacheOnDeallocate;
  }

  @Override
  public boolean isRaiseExceptionOnSilentRollback() {
    return raiseExceptionOnSilentRollback;
  }

  @Override
  public void setRaiseExceptionOnSilentRollback(boolean raiseExceptionOnSilentRollback) {
    this.raiseExceptionOnSilentRollback = raiseExceptionOnSilentRollback;
  }

  protected boolean hasNotifications() {
    return notifications.size() > 0;
  }

  @Override
  public final Map<String,String> getParameterStatuses() {
    return Collections.unmodifiableMap(parameterStatuses);
  }

  @Override
  public final String getParameterStatus(String parameterName) {
    return parameterStatuses.get(parameterName);
  }

  /**
   * Update the parameter status map in response to a new ParameterStatus
   * wire protocol message.
   *
   * <p>The server sends ParameterStatus messages when GUC_REPORT settings are
   * initially assigned and whenever they change.</p>
   *
   * <p>A future version may invoke a client-defined listener class at this point,
   * so this should be the only access path.</p>
   *
   * <p>Keys are case-insensitive and case-preserving.</p>
   *
   * <p>The server doesn't provide a way to report deletion of a reportable
   * parameter so we don't expose one here.</p>
   *
   * @param parameterName case-insensitive case-preserving name of parameter to create or update
   * @param parameterStatus new value of parameter
   * @see com.amazon.redshift.RedshiftConnection#getParameterStatuses
   * @see com.amazon.redshift.RedshiftConnection#getParameterStatus
   */
  protected void onParameterStatus(String parameterName, String parameterStatus) {
    if (parameterName == null || parameterName.equals("")) {
      throw new IllegalStateException("attempt to set GUC_REPORT parameter with null or empty-string name");
    }

    parameterStatuses.put(parameterName, parameterStatus);
  }
  
  /**
   * Close the last active ring buffer thread.
   */
  @Override
  public void closeRingBufferThread(RedshiftRowsBlockingQueue<Tuple> queueRows, Thread ringBufferThread) {
  	// Does nothing
  }
  
  /**
   * Check for a running ring buffer thread. 
   * 
   * @return returns true if Ring buffer thread is running, otherwise false.
   */
  @Override
  public boolean isRingBufferThreadRunning() {
  	return false;
  }
  
  @Override
  public void closeStatementAndPortal() {
  	// Do nothing
  }
  
}
