/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift;

import com.amazon.redshift.fastpath.Fastpath;
import com.amazon.redshift.jdbc.AutoSave;
import com.amazon.redshift.jdbc.PreferQueryMode;
import com.amazon.redshift.largeobject.LargeObjectManager;
import com.amazon.redshift.util.RedshiftObject;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * This interface defines the public Redshift extensions to java.sql.Connection. All Connections
 * returned by the Redshift driver implement RedshiftConnection.
 */
public interface RedshiftConnection {

  /**
   * Creates an {@link Array} wrapping <i>elements</i>. This is similar to
   * {@link java.sql.Connection#createArrayOf(String, Object[])}, but also
   * provides support for primitive arrays.
   *
   * @param typeName
   *          The SQL name of the type to map the <i>elements</i> to.
   *          Must not be {@code null}.
   * @param elements
   *          The array of objects to map. A {@code null} value will result in
   *          an {@link Array} representing {@code null}.
   * @return An {@link Array} wrapping <i>elements</i>.
   * @throws SQLException
   *           If for some reason the array cannot be created.
   * @see java.sql.Connection#createArrayOf(String, Object[])
   */
  Array createArrayOf(String typeName, Object elements) throws SQLException;

  /**
   * This method returns any notifications that have been received since the last call to this
   * method. Returns null if there have been no notifications.
   *
   * @return notifications that have been received
   * @throws SQLException if something wrong happens
   * @since 7.3
   */
  RedshiftNotification[] getNotifications() throws SQLException;

  Long getBytesReadFromStream();

  /**
   * This method returns any notifications that have been received since the last call to this
   * method. Returns null if there have been no notifications. A timeout can be specified so the
   * driver waits for notifications.
   *
   * @param timeoutMillis when 0, blocks forever. when &gt; 0, blocks up to the specified number of millies
   *        or until at least one notification has been received. If more than one notification is
   *        about to be received, these will be returned in one batch.
   * @return notifications that have been received
   * @throws SQLException if something wrong happens
   * @since 43
   */
  RedshiftNotification[] getNotifications(int timeoutMillis) throws SQLException;


  /**
   * This returns the LargeObject API for the current connection.
   *
   * @return LargeObject API for the current connection
   * @throws SQLException if something wrong happens
   * @since 7.3
   */
  LargeObjectManager getLargeObjectAPI() throws SQLException;

  /**
   * This returns the Fastpath API for the current connection.
   *
   * @return Fastpath API for the current connection
   * @throws SQLException if something wrong happens
   * @since 7.3
   * @deprecated This API is somewhat obsolete, as one may achieve similar performance
   *         and greater functionality by setting up a prepared statement to define
   *         the function call. Then, executing the statement with binary transmission of parameters
   *         and results substitutes for a fast-path function call.
   */
  @Deprecated
  Fastpath getFastpathAPI() throws SQLException;

  /**
   * <p>This allows client code to add a handler for one of com.amazon.redshift's more unique data types.</p>
   *
   * <p><b>NOTE:</b> This is not part of JDBC, but an extension.</p>
   *
   * <p>The best way to use this is as follows:</p>
   *
   * <pre>
   * ...
   * ((com.amazon.redshift.RedshiftConnection)myconn).addDataType("mytype", my.class.name.class);
   * ...
   * </pre>
   *
   * <p>where myconn is an open Connection to com.amazon.redshift.</p>
   *
   * <p>The handling class must extend com.amazon.redshift.util.RedshiftObject</p>
   *
   * @param type the Redshift type to register
   * @param klass the class implementing the Java representation of the type; this class must
   *        implement {@link com.amazon.redshift.util.RedshiftObject}).
   * @throws SQLException if <code>klass</code> does not implement
   *         {@link com.amazon.redshift.util.RedshiftObject}).
   * @see com.amazon.redshift.util.RedshiftObject
   * @since 8.0
   */
  void addDataType(String type, Class<? extends RedshiftObject> klass) throws SQLException;

  /**
   * Set the default statement reuse threshold before enabling server-side prepare. See
   * {@link com.amazon.redshift.RedshiftStatement#setPrepareThreshold(int)} for details.
   *
   * @param threshold the new threshold
   * @since build 302
   */
  void setPrepareThreshold(int threshold);

  /**
   * Get the default server-side prepare reuse threshold for statements created from this
   * connection.
   *
   * @return the current threshold
   * @since build 302
   */
  int getPrepareThreshold();

  /**
   * Set the default fetch size for statements created from this connection.
   *
   * @param fetchSize new default fetch size
   * @throws SQLException if specified negative <code>fetchSize</code> parameter
   * @see Statement#setFetchSize(int)
   */
  void setDefaultFetchSize(int fetchSize) throws SQLException;

  /**
   * Get the default fetch size for statements created from this connection.
   *
   * @return current state for default fetch size
   * @see RedshiftProperty#DEFAULT_ROW_FETCH_SIZE
   * @see Statement#getFetchSize()
   */
  int getDefaultFetchSize();

  /**
   * Return the process ID (PID) of the backend server process handling this connection.
   *
   * @return PID of backend server process.
   */
  int getBackendPID();

  /**
   * Sends a query cancellation for this connection.
   * @throws SQLException if there are problems cancelling the query
   */
  void cancelQuery() throws SQLException;

  /**
   * Return the given string suitably quoted to be used as an identifier in an SQL statement string.
   * Quotes are added only if necessary (i.e., if the string contains non-identifier characters or
   * would be case-folded). Embedded quotes are properly doubled.
   *
   * @param identifier input identifier
   * @return the escaped identifier
   * @throws SQLException if something goes wrong
   */
  String escapeIdentifier(String identifier) throws SQLException;

  /**
   * Return the given string suitably quoted to be used as a string literal in an SQL statement
   * string. Embedded single-quotes and backslashes are properly doubled. Note that quote_literal
   * returns null on null input.
   *
   * @param literal input literal
   * @return the quoted literal
   * @throws SQLException if something goes wrong
   */
  String escapeLiteral(String literal) throws SQLException;

  /**
   * <p>Returns the query mode for this connection.</p>
   *
   * <p>When running in simple query mode, certain features are not available: callable statements,
   * partial result set fetch, bytea type, etc.</p>
   * <p>The list of supported features is subject to change.</p>
   *
   * @return the preferred query mode
   * @see PreferQueryMode
   */
  PreferQueryMode getPreferQueryMode();

  /**
   * Connection configuration regarding automatic per-query savepoints.
   *
   * @return connection configuration regarding automatic per-query savepoints
   * @see RedshiftProperty#AUTOSAVE
   */
  AutoSave getAutosave();

  /**
   * Configures if connection should use automatic savepoints.
   * @param autoSave connection configuration regarding automatic per-query savepoints
   * @see RedshiftProperty#AUTOSAVE
   */
  void setAutosave(AutoSave autoSave);


  /**
   * <p>Returns the current values of all parameters reported by the server.</p>
   *
   * <p>Redshift reports values for a subset of parameters (GUCs) to the client
   * at connect-time, then sends update messages whenever the values change
   * during a session. PgJDBC records the latest values and exposes it to client
   * applications via <code>getParameterStatuses()</code>.</p>
   *
   * <p>PgJDBC exposes individual accessors for some of these parameters as
   * listed below. They are more backwarrds-compatible and should be preferred
   * where possible.</p>
   *
   * <p>Not all parameters are reported, only those marked
   * <code>GUC_REPORT</code> in the source code. The <code>pg_settings</code>
   * view does not expose information about which parameters are reportable.
   * PgJDBC's map will only contain the parameters the server reports values
   * for, so you cannot use this method as a substitute for running a
   * <code>SHOW paramname;</code> or <code>SELECT
   * current_setting('paramname');</code> query for arbitrary parameters.</p>
   *
   * <p>Parameter names are <i>case-insensitive</i> and <i>case-preserving</i>
   * in this map, like in Redshift itself. So <code>DateStyle</code> and
   * <code>datestyle</code> are the same key.</p>
   *
   * <p>
   *  As of PostgreSQL 11 the reportable parameter list, and related PgJDBC
   *  interfaces or accesors, are:
   * </p>
   *
   * <ul>
   *  <li>
   *    <code>application_name</code> -
   *    {@link java.sql.Connection#getClientInfo()},
   *    {@link java.sql.Connection#setClientInfo(java.util.Properties)}
   *    and <code>ApplicationName</code> connection property.
   *  </li>
   *  <li>
   *    <code>client_encoding</code> - PgJDBC always sets this to <code>UTF8</code>.
   *    See <code>allowEncodingChanges</code> connection property.
   *  </li>
   *  <li><code>DateStyle</code> - PgJDBC requires this to always be set to <code>ISO</code></li>
   *  <li><code>standard_conforming_strings</code> - indirectly via {@link #escapeLiteral(String)}</li>
   *  <li>
   *    <code>TimeZone</code> - set from JDK timezone see {@link java.util.TimeZone#getDefault()}
   *    and {@link java.util.TimeZone#setDefault(TimeZone)}
   *  </li>
   *  <li><code>integer_datetimes</code></li>
   *  <li><code>IntervalStyle</code></li>
   *  <li><code>server_encoding</code></li>
   *  <li><code>server_version</code></li>
   *  <li><code>is_superuser</code> </li>
   *  <li><code>session_authorization</code></li>
   * </ul>
   *
   * <p>Note that some PgJDBC operations will change server parameters
   * automatically.</p>
   *
   * @return unmodifiable map of case-insensitive parameter names to parameter values
   * @since 42.2.6
   */
  Map<String,String> getParameterStatuses();

  /**
   * Shorthand for getParameterStatuses().get(...) .
   *
   * @param parameterName case-insensitive parameter name
   * @return parameter value if defined, or null if no parameter known
   * @see #getParameterStatuses
   * @since 42.2.6
   */
  String getParameterStatus(String parameterName);
}
