/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.Arrays;

/**
 * All connection parameters that can be either set in JDBC URL, in Driver properties or in
 * datasource setters.
 */
public enum RedshiftProperty {

  /**
   * When using the V3 protocol the driver monitors changes in certain server configuration
   * parameters that should not be touched by end users. The {@code client_encoding} setting is set
   * by the driver and should not be altered. If the driver detects a change it will abort the
   * connection.
   */
  ALLOW_ENCODING_CHANGES(
    "allowencodingchanges",
    "true",
    "Allow for changes in client_encoding"),

  /**
   * The application name.
   */
  APPLICATION_NAME(
    "applicationname",
    null,
    "Name of the Application"),

  /**
   * Assume the server is at least that version.
   */
  ASSUME_MIN_SERVER_VERSION(
    "assumeminserverversion",
    null,
    "Assume the server is at least that version"),

  /**
   * The authentication profile referring to connection props as JSON in the coral service.
   */
  AUTH_PROFILE(
    "authprofile",
    null,
    "Authentication profile having connection props as JSON"),
  
  /**
   * Specifies what the driver should do if a query fails. In {@code autosave=always} mode, JDBC driver sets a savepoint before each query,
   * and rolls back to that savepoint in case of failure. In {@code autosave=never} mode (default), no savepoint dance is made ever.
   * In {@code autosave=conservative} mode, savepoint is set for each query, however the rollback is done only for rare cases
   * like 'cached statement cannot change return type' or 'statement XXX is not valid' so JDBC driver rollsback and retries
   */
  AUTOSAVE(
    "autosave",
    "never",
    "Specifies what the driver should do if a query fails. In autosave=always mode, JDBC driver sets a savepoint before each query, "
        + "and rolls back to that savepoint in case of failure. In autosave=never mode (default), no savepoint dance is made ever. "
        + "In autosave=conservative mode, safepoint is set for each query, however the rollback is done only for rare cases"
        + " like 'cached statement cannot change return type' or 'statement XXX is not valid' so JDBC driver rollsback and retries",
    false,
    new String[] {"always", "never", "conservative"}),

  /**
   * Use binary format for sending and receiving data if possible.
   */
  BINARY_TRANSFER(
    "binarytransfer",
    "true",
    "Use binary format for sending and receiving data if possible"),

  /**
   * Comma separated list of types to disable binary transfer. Either OID numbers or names.
   * Overrides values in the driver default set and values set with binaryTransferEnable.
   */
  BINARY_TRANSFER_DISABLE(
    "binarytransferdisable",
    "",
    "Comma separated list of types to disable binary transfer. Either OID numbers or names. Overrides values in the driver default set and values set with binaryTransferEnable."),

  /**
   * Comma separated list of types to enable binary transfer. Either OID numbers or names
   */
  BINARY_TRANSFER_ENABLE(
    "binarytransferenable",
    "",
    "Comma separated list of types to enable binary transfer. Either OID numbers or names"),

  /**
   * Cancel command is sent out of band over its own connection, so cancel message can itself get
   * stuck.
   * This property controls "connect timeout" and "socket timeout" used for cancel commands.
   * The timeout is specified in seconds. Default value is 10 seconds.
   */
  CANCEL_SIGNAL_TIMEOUT(
    "cancelsignaltimeout",
    "10",
    "The timeout that is used for sending cancel command."),

  /**
   * Determine whether SAVEPOINTS used in AUTOSAVE will be released per query or not
   */
  CLEANUP_SAVEPOINTS(
    "cleanupsavepoints",
    "false",
    "Determine whether SAVEPOINTS used in AUTOSAVE will be released per query or not",
    false,
    new String[] {"true", "false"}),

  /**
   * <p>The compression method used.</p>
   */
  COMPRESSION(
          "compression",
          "off",
          "The compression method used for wire protocol communication between Redshift server and the client/driver"),

  /**
   * <p>The timeout value used for socket connect operations. If connecting to the server takes longer
   * than this value, the connection is broken.</p>
   *
   * <p>The timeout is specified in seconds and a value of zero means that it is disabled.</p>
   */
  CONNECT_TIMEOUT(
    "connecttimeout",
    "10",
    "The timeout value used for socket connect operations."),

  /**
   * <p>Configures the session level timezone to one of the two logical values LOCAL or SERVER</p>
   */
  CONNECTION_TIMEZONE(
    "connectiontimezone",
            "LOCAL",
            "Configures the session level timezone to one of the two logical values LOCAL or SERVER"),

  /**
   * Specify the schema (or several schema separated by commas) to be set in the search-path. This schema will be used to resolve
   * unqualified object names used in statements over this connection.
   */
  CURRENT_SCHEMA(
    "currentschema",
    null,
    "Specify the schema (or several schema separated by commas) to be set in the search-path"),

  /**
   * Specifies the maximum number of fields to be cached per connection. A value of {@code 0} disables the cache.
   */
  DATABASE_METADATA_CACHE_FIELDS(
    "databasemetadatacachefields",
    "65536",
    "Specifies the maximum number of fields to be cached per connection. A value of {@code 0} disables the cache."),

  /**
   * Specifies the maximum size (in megabytes) of fields to be cached per connection. A value of {@code 0} disables the cache.
   */
  DATABASE_METADATA_CACHE_FIELDS_MIB(
    "databasemetadatacachefieldsmib",
    "5",
    "Specifies the maximum size (in megabytes) of fields to be cached per connection. A value of {@code 0} disables the cache."),

  /**
   * Returns metadata for the connected database only.
   * Application is ready to accept metadata from all databases,
   *  can set the value of this parameter "false".
   *  Default value is "true" means application gets metadata from single
   *  databases. 
   */
  DATABASE_METADATA_CURRENT_DB_ONLY(
    "databasemetadatacurrentdbonly",
    "true",
    "Control the behavior of metadata API to return data from all accessible databases or only from connected database"),
  
  /**
   * Default parameter for {@link java.sql.Statement#getFetchSize()}. A value of {@code 0} means
   * that need fetch all rows at once
   */
  DEFAULT_ROW_FETCH_SIZE(
    "defaultrowfetchsize",
    "0",
    "Positive number of rows that should be fetched from the database when more rows are needed for ResultSet by each fetch iteration"),

  /**
   * Added for backward compatibility.
   * 
   */
  BLOCKING_ROWS_MODE(
    "blockingrowsmode",
    "0",
    "Positive number of rows that should be fetched from the database when more rows are needed for ResultSet by each fetch iteration"),
  
  /**
   * Enable optimization that disables column name sanitiser.
   */
  DISABLE_COLUMN_SANITISER(
    "disablecolumnsanitiser",
    "false",
    "Enable optimization that disables column name sanitiser"),

  /**
   * This option specifies whether the driver submits a new database query when using the
	 * Connection.isValid() method to determine whether the database connection is active.
	 * 
   * true: The driver does not submit a query when using Connection.isValid() to
   * determine whether the database connection is active. This may cause the driver
	 * to incorrectly identify the database connection as active if the database server
	 * has shut down unexpectedly.
	 * 
	 * false: The driver submits a query when using Connection.isValid() to
	 * determine whether the database connection is active.
   * When using the V3 protocol the driver monitors changes in certain server configuration
   * parameters that should not be touched by end users. The {@code client_encoding} setting is set
   * by the driver and should not be altered. If the driver detects a change it will abort the
   * connection.
   */
  DISABLE_ISVALID_QUERY(
    "disableisvalidquery",
    "false",
    "Disable isValid query"),
  
  /**
   * The Redshift fetch rows using a ring buffer on a separate thread.
   * 
   */
  ENABLE_FETCH_RING_BUFFER("enablefetchringbuffer",
  							"true",
  							"The Redshift fetch rows using a ring buffer on a separate thread"),
  
  /**
   * Use generated statement name cursor for prepared statements. 
   * 
   */
  ENABLE_GENERATED_NAME_FOR_PREPARED_STATEMENT("enablegeneratedname",
  							"true",
  							"The Redshift uses generated statement name and portal name"),
  
  /**
   * "true" means driver supports multiple SQL commands (semicolon separated) in a Statement object.
   * "false" means driver throws an exception when see multiple SQL commands.
   * Default value is "true".
   */
  ENABLE_MULTI_SQL_SUPPORT(
    "enablemultisqlsupport",
    "true",
    "Control the behavior of semicolon separated SQL commands in a Statement"),
  
  /**
   * The statement cache enable/disable.
   * 
   */
  ENABLE_STATEMENT_CACHE("enablestatementcache",
  							"false",
  							"The Redshift statement cache using SQL as key"),
  
  
  
  /**
   * Specifies how the driver transforms JDBC escape call syntax into underlying SQL, for invoking procedures or functions. (backend &gt;= 11)
   * In {@code escapeSyntaxCallMode=select} mode (the default), the driver always uses a SELECT statement (allowing function invocation only).
   * In {@code escapeSyntaxCallMode=callIfNoReturn} mode, the driver uses a CALL statement (allowing procedure invocation) if there is no return parameter specified, otherwise the driver uses a SELECT statement.
   * In {@code escapeSyntaxCallMode=call} mode, the driver always uses a CALL statement (allowing procedure invocation only).
   */
  ESCAPE_SYNTAX_CALL_MODE(
    "escapesyntaxcallmode",
    "call",
    "Specifies how the driver transforms JDBC escape call syntax into underlying SQL, for invoking procedures or functions. (backend >= 11)"
      + "In escapeSyntaxCallMode=select mode (the default), the driver always uses a SELECT statement (allowing function invocation only)."
      + "In escapeSyntaxCallMode=callIfNoReturn mode, the driver uses a CALL statement (allowing procedure invocation) if there is no return parameter specified, otherwise the driver uses a SELECT statement."
      + "In escapeSyntaxCallMode=call mode, the driver always uses a CALL statement (allowing procedure invocation only).",
    false,
    new String[] {"select", "callIfNoReturn", "call"}),

  /**
   * Specifies size of buffer during fetching result set. Can be specified as specified size or
   * percent of heap memory.
   */
  FETCH_RING_BUFFER_SIZE(
    "fetchringbuffersize",
    "1G",
    "Specifies size of ring buffer during fetching result set. Can be specified as specified size or percent of heap memory."),
  
  /**
   * Force one of
   * <ul>
   * <li>SSPI (Windows transparent single-sign-on)</li>
   * <li>GSSAPI (Kerberos, via JSSE)</li>
   * </ul>
   * to be used when the server requests Kerberos or SSPI authentication.
   */
  GSS_LIB(
    "gsslib",
    "auto",
    "Force SSSPI or GSSAPI",
    false,
    new String[] {"auto", "sspi", "gssapi"}),

  /**
   * Enable mode to filter out the names of database objects for which the current user has no privileges
   * granted from appearing in the DatabaseMetaData returned by the driver.
   */
  HIDE_UNPRIVILEGED_OBJECTS(
    "hideunprivilegedobjects",
    "false",
    "Enable hiding of database objects for which the current user has no privileges granted from the DatabaseMetaData"),

  HOST_RECHECK_SECONDS(
    "hostrecheckseconds",
    "10",
    "Specifies period (seconds) after which the host status is checked again in case it has changed"),

  /**
   * The JDBC INI file name.
   */
  INI_FILE("inifile",
  					null,
  					"The JDBC INI file. Easy to configure connection properties."),

  /**
   * The JDBC INI file section name.
   * Section name to use for connection configuration.
   */
  INI_SECTION("inisection",
  					null,
  					"The JDBC INI file section name."),
  
  /**
   * Specifies the name of the JAAS system or application login configuration.
   */
  JAAS_APPLICATION_NAME(
    "jaasapplicationname",
    null,
    "Specifies the name of the JAAS system or application login configuration."),

  /**
   * Flag to enable/disable obtaining a GSS credential via JAAS login before authenticating.
   * Useful if setting system property javax.security.auth.useSubjectCredsOnly=false
   * or using native GSS with system property sun.security.jgss.native=true
   */
  JAAS_LOGIN(
    "jaaslogin",
    "true",
    "Login with JAAS before doing GSSAPI authentication"),

  /**
   * The Kerberos service name to use when authenticating with GSSAPI. This is equivalent to libpq's
   * PGKRBSRVNAME environment variable.
   */
  KERBEROS_SERVER_NAME(
    "kerberosservername",
    null,
    "The Kerberos service name to use when authenticating with GSSAPI."),

  LOAD_BALANCE_HOSTS(
    "loadbalancehosts",
    "false",
    "If disabled hosts are connected in the given order. If enabled hosts are chosen randomly from the set of suitable candidates"),

  LOG_PATH(
      "logpath",
      null,
      "File Path output of the Logger"),

  MAX_LOG_FILE_SIZE(
      "maxlogfilesize",
      null,
      "Maximum single log file size"),

  MAX_LOG_FILE_COUNT(
      "maxlogfilecount",
      null,
      "Maximum number of log files"),
  
  /**
   * Added for backward compatibility.
   */
  LOG_LEVEL(
      "loglevel",
      null,
      "Log level of the driver",
      false,
      new String[] {"OFF", "FATAL", "ERROR", "WARNING", "INFO", "FUNCTION", "DEBUG", "TRACE"}),

  /**
   * Added for backward compatibility.
   */
  DSI_LOG_LEVEL(
      "dsiloglevel",
      null,
      "Log level of the driver",
      false,
      new String[] {"OFF", "FATAL", "ERROR", "WARNING", "INFO", "FUNCTION", "DEBUG", "TRACE"}),
  
  /**
   * Specify how long to wait for establishment of a database connection. The timeout is specified
   * in seconds.
   */
  LOGIN_TIMEOUT(
    "logintimeout",
    "0",
    "Specify how long to wait for establishment of a database connection."),

  /**
   * Whether to include full server error detail in exception messages.
   */
  LOG_SERVER_ERROR_DETAIL(
    "logservererrordetail",
    "true",
    "Include full server error detail in exception messages. If disabled then only the error itself will be included."),

  /**
   * When connections that are not explicitly closed are garbage collected, log the stacktrace from
   * the opening of the connection to trace the leak source.
   */
  LOG_UNCLOSED_CONNECTIONS(
    "logunclosedconnections",
    "false",
    "When connections that are not explicitly closed are garbage collected, log the stacktrace from the opening of the connection to trace the leak source"),

  /**
   * Specifies size of buffer during fetching result set. Can be specified as specified size or
   * percent of heap memory.
   */
  MAX_RESULT_BUFFER(
    "maxresultbuffer",
    null,
    "Specifies size of buffer during fetching result set. Can be specified as specified size or percent of heap memory."),

  /**
   * Specify 'options' connection initialization parameter.
   * The value of this parameter may contain spaces and other special characters or their URL representation.
   */
  OPTIONS(
    "options",
    null,
    "Specify 'options' connection initialization parameter."),

  /**
   *    * Override Schema Pattern Type used in getTables()
   */
  OVERRIDE_SCHEMA_PATTERN_TYPE(
          "overrideschemapatterntype",
          null,
          "Override the type of query used in getTables calls"),

  /**
   * Password to use when authenticating.
   */
  PASSWORD(
    "password",
    null,
    "Password to use when authenticating.",
    false),

  /**
   * Password to use when authenticating. It's an alias for the Password.
   */
  PWD(
    "pwd",
    null,
    "Password to use when authenticating.",
    false),

  /**
   * Set the query group on a connection.
   */
  QUERY_GROUP(
    "querygroup",
    null,
    "Assign a query to a queue at runtime by assigning your query to the appropriate query group"),
  
  /**
   * Database name to connect to (may be specified directly in the JDBC URL).
   */
  DBNAME(
    "dbname",
    null,
    "Database name to connect to (may be specified directly in the JDBC URL)",
    true),

  /**
   * Hostname of the Redshift server (may be specified directly in the JDBC URL).
   */
  HOST(
    "host",
    null,
    "Hostname of the Redshift server (may be specified directly in the JDBC URL)",
    false),

  /**
   * Port of the Redshift server (may be specified directly in the JDBC URL).
   */
  PORT(
    "port",
    null,
    "Port of the Redshift server (may be specified directly in the JDBC URL)"),

  /**
   * <p>Specifies which mode is used to execute queries to database: simple means ('Q' execute, no parse, no bind, text mode only),
   * extended means always use bind/execute messages, extendedForPrepared means extended for prepared statements only,
   * extendedCacheEverything means use extended protocol and try cache every statement (including Statement.execute(String sql)) in a query cache.</p>
   *
   * <p>This mode is meant for debugging purposes and/or for cases when extended protocol cannot be used (e.g. logical replication protocol)</p>
   */
  PREFER_QUERY_MODE(
    "preferquerymode",
    "extended",
    "Specifies which mode is used to execute queries to database: simple means ('Q' execute, no parse, no bind, text mode only), "
        + "extended means always use bind/execute messages, extendedForPrepared means extended for prepared statements only, "
        + "extendedCacheEverything means use extended protocol and try cache every statement (including Statement.execute(String sql)) in a query cache.", false,
    new String[] {"extended", "extendedForPrepared", "extendedCacheEverything", "simple"}),

  /**
   * Specifies the maximum number of entries in cache of prepared statements. A value of {@code 0}
   * disables the cache.
   */
  PREPARED_STATEMENT_CACHE_QUERIES(
    "preparedstatementcachequeries",
    "256",
    "Specifies the maximum number of entries in per-connection cache of prepared statements. A value of {@code 0} disables the cache."),

  /**
   * Specifies the maximum size (in megabytes) of the prepared statement cache. A value of {@code 0}
   * disables the cache.
   */
  PREPARED_STATEMENT_CACHE_SIZE_MIB(
    "preparedstatementcachesizemib",
    "5",
    "Specifies the maximum size (in megabytes) of a per-connection prepared statement cache. A value of {@code 0} disables the cache."),

  /**
   * Sets the default threshold for enabling server-side prepare. A value of {@code -1} stands for
   * forceBinary
   */
  PREPARE_THRESHOLD(
    "preparethreshold",
    "5",
    "Statement prepare threshold. A value of {@code -1} stands for forceBinary"),

  /**
   * Force use of a particular protocol version when connecting, if set, disables protocol version
   * fallback.
   */
  PROTOCOL_VERSION(
    "protocolversion",
    null,
    "Force use of a particular protocol version when connecting, currently only version 3 is supported.",
    false,
    new String[] {"3"}),

  /**
   * Certain database versions perform a silent rollback instead of commit in case the transaction was in a failed state.
   */
  RAISE_EXCEPTION_ON_SILENT_ROLLBACK(
    "raiseexceptiononsilentrollback",
    "false",
    "Certain database versions perform a silent rollback instead of commit in case the transaction was in a failed state"),

  /**
   * Puts this connection in read-only mode.
   */
  READ_ONLY(
    "readonly",
    "false",
    "Puts this connection in read-only mode"),

  /**
   * Connection parameter to control behavior when
   * {@link Connection#setReadOnly(boolean)} is set to {@code true}.
   */
  READ_ONLY_MODE(
    "readonlymode",
    "always", // transaction
    "Controls the behavior when a connection is set to be read only, one of 'ignore', 'transaction', or 'always' "
      + "When 'ignore', setting readOnly has no effect. "
      + "When 'transaction' setting readOnly to 'true' will cause transactions to BEGIN READ ONLY if autocommit is 'false'. "
      + "When 'always' setting readOnly to 'true' will set the session to READ ONLY if autoCommit is 'true' "
      + "and the transaction to BEGIN READ ONLY if autocommit is 'false'.",
    false,
    new String[] {"ignore", "transaction", "always"}),

  /**
   * Socket read buffer size (SO_RECVBUF). A value of {@code -1}, which is the default, means system
   * default.
   */
  RECEIVE_BUFFER_SIZE(
    "receivebuffersize",
    "-1",
    "Socket read buffer size"),

  /**
   * <p>Connection parameter passed in the startup message. This parameter accepts two values; "true"
   * and "database". Passing "true" tells the backend to go into walsender mode, wherein a small set
   * of replication commands can be issued instead of SQL statements. Only the simple query protocol
   * can be used in walsender mode. Passing "database" as the value instructs walsender to connect
   * to the database specified in the dbname parameter, which will allow the connection to be used
   * for logical replication from that database.</p>
   * <p>Parameter should be use together with {@link RedshiftProperty#ASSUME_MIN_SERVER_VERSION} with
   * parameter &gt;= 9.4 (backend &gt;= 9.4)</p>
   */
  REPLICATION(
    "replication",
    null,
    "Connection parameter passed in startup message, one of 'true' or 'database' "
      + "Passing 'true' tells the backend to go into walsender mode, "
      + "wherein a small set of replication commands can be issued instead of SQL statements. "
      + "Only the simple query protocol can be used in walsender mode. "
      + "Passing 'database' as the value instructs walsender to connect "
      + "to the database specified in the dbname parameter, "
      + "which will allow the connection to be used for logical replication "
      + "from that database. "
      + "(backend >= 9.4)"),

  /**
   * Configure optimization to enable batch insert re-writing.
   */
  REWRITE_BATCHED_INSERTS(
    "rewritebatchedinserts",
    "false",
    "Enable optimization to rewrite and collapse compatible INSERT statements that are batched."),

  /**
   * Configure optimization to batch insert size re-writing.
   * This must be power of 2.
   */
  REWRITE_BATCHED_INSERTS_SIZE(
    "rewritebatchedinsertssize",
    "128",
    "Enable optimization size to rewrite and collapse compatible INSERT statements that are batched. This must be power of 2"),
  
  /**
   * Socket write buffer size (SO_SNDBUF). A value of {@code -1}, which is the default, means system
   * default.
   */
  SEND_BUFFER_SIZE(
    "sendbuffersize",
    "-1",
    "Socket write buffer size"),

  /**
   * Socket factory used to create socket. A null value, which is the default, means system default.
   */
  SOCKET_FACTORY(
    "socketfactory",
    null,
    "Specify a socket factory for socket creation"),

  /**
   * The String argument to give to the constructor of the Socket Factory.
   * @deprecated use {@code ..Factory(Properties)} constructor.
   */
  @Deprecated
  SOCKET_FACTORY_ARG(
    "socketfactoryarg",
    null,
    "Argument forwarded to constructor of SocketFactory class."),

  /**
   * The timeout value used for socket read operations. If reading from the server takes longer than
   * this value, the connection is closed. This can be used as both a brute force global query
   * timeout and a method of detecting network problems. The timeout is specified in seconds and a
   * value of zero means that it is disabled.
   */
  SOCKET_TIMEOUT(
    "sockettimeout",
    "0",
    "The timeout value used for socket read operations."),

  /**
   * Control use of SSL: empty or {@code true} values imply {@code sslmode==verify-full}
   */
  SSL(
    "ssl",
    "true",
    "Control use of SSL (any non-null value causes SSL to be required)"),

  /**
   * File containing the SSL Certificate. Default will be the file {@code redshift.crt} in {@code
   * $HOME/.redshift} (*nix) or {@code %APPDATA%\redshift} (windows).
   */
  SSL_CERT(
    "sslcert",
    null,
    "The location of the client's SSL certificate"),

  /**
   * Classname of the SSL Factory to use (instance of {@code javax.net.ssl.SSLSocketFactory}).
   */
  SSL_FACTORY(
    "sslfactory",
    null,
    "Provide a SSLSocketFactory class when using SSL."),

  /**
   * The String argument to give to the constructor of the SSL Factory.
   * @deprecated use {@code ..Factory(Properties)} constructor.
   */
  @Deprecated
  SSL_FACTORY_ARG(
    "sslfactoryarg",
    null,
    "Argument forwarded to constructor of SSLSocketFactory class."),

  /**
   * Classname of the SSL HostnameVerifier to use (instance of {@code
   * javax.net.ssl.HostnameVerifier}).
   */
  SSL_HOSTNAME_VERIFIER(
    "sslhostnameverifier",
    null,
    "A class, implementing javax.net.ssl.HostnameVerifier that can verify the server"),

  /**
   * File containing the SSL Key. Default will be the file {@code redshift.pk8} in {@code
   * $HOME/.redshift} (*nix) or {@code %APPDATA%\redshift} (windows).
   */
  SSL_KEY(
    "sslkey",
    null,
    "The location of the client's PKCS#8 SSL key"),

  /**
   * Parameter governing the use of SSL. The allowed values are {@code disable}, {@code allow},
   * {@code prefer}, {@code require}, {@code verify-ca}, {@code verify-full}.
   * If {@code ssl} property is empty or set to {@code true} it implies {@code verify-full}.
   * Default mode is "require"
   */
  SSL_MODE(
    "sslmode",
    null,
    "Parameter governing the use of SSL",
    false,
    new String[] {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}),

  /** 
   * Added for backward compatibility.
   */
  AUTH_MECH(
      "authmech",
      null,
      "Parameter governing the use of SSL. Alias for sslMode",
      false,
      new String[] {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}),
  
  /**
   * The SSL password to use in the default CallbackHandler.
   */
  SSL_PASSWORD(
    "sslpassword",
    null,
    "The password for the client's ssl key (ignored if sslpasswordcallback is set)"),


  /**
   * The classname instantiating {@code javax.security.auth.callback.CallbackHandler} to use.
   */
  SSL_PASSWORD_CALLBACK(
    "sslpasswordcallback",
    null,
    "A class, implementing javax.security.auth.callback.CallbackHandler that can handle PassworCallback for the ssl password."),

  /**
   * File containing the root certificate when validating server ({@code sslmode} = {@code
   * verify-ca} or {@code verify-full}). Default will be the file {@code root.crt} in {@code
   * $HOME/.redshift} (*nix) or {@code %APPDATA%\redshift} (windows).
   */
  SSL_ROOT_CERT(
    "sslrootcert",
    null,
    "The location of the root certificate for authenticating the server."),
  
  /**
   * The SSL Truststore path key.
   */
  SSL_TRUSTSTORE_PATH_KEY(
    "ssltruststorepath",
    null,
    "The SSL Truststore path key."),
  

  /**
   * The SSL Truststore path key.
   */
  SSL_TRUSTSTORE_PWD_KEY(
    "ssltruststore ",
    null,
    "The SSL Truststore password key."),
  
  /**
   * Specifies the name of the SSPI service class that forms the service class part of the SPN. The
   * default, {@code REDSHIFT}, is almost always correct.
   */
  SSPI_SERVICE_CLASS(
    "sspiserviceclass",
    "REDSHIFT",
    "The Windows SSPI service class for SPN"),

  /**
   * Bind String to either {@code unspecified} or {@code varchar}. Default is {@code varchar} for
   * 8.0+ backends.
   */
  STRING_TYPE(
    "stringtype",
    "unspecified",
    "The type to bind String parameters as (usually 'varchar', 'unspecified' allows implicit casting to other types)",
    false,
    new String[] {"unspecified", "varchar"}),

  TARGET_SERVER_TYPE(
    "targetservertype",
    "any",
    "Specifies what kind of server to connect",
    false,
    new String [] {"any", "primary", "master", "slave", "secondary",  "preferSlave", "preferSecondary"}),

  /**
   * Enable or disable TCP keep-alive. The default is {@code true}.
   */
  TCP_KEEP_ALIVE(
    "tcpkeepalive",
    "true",
    "Enable or disable TCP keep-alive. The default is {@code true}."),

  /**
   * Specifies the length to return for types of unknown length.
   */
  UNKNOWN_LENGTH(
    "unknownlength",
    Integer.toString(Integer.MAX_VALUE),
    "Specifies the length to return for types of unknown length"),

  /**
   * Username to connect to the database as.
   */
  USER(
    "user",
    null,
    "Username to connect to the database as.",
    true),

  /**
   * Username to connect to the database as. It's an alias for the USER.
   */
  UID(
    "uid",
    null,
    "Username to connect to the database as.",
    true),
  
  /**
   * Use SPNEGO in SSPI authentication requests.
   */
  USE_SPNEGO(
    "usespnego",
    "false",
    "Use SPNEGO in SSPI authentication requests"),
  
  // IAM properties
  /**
   * The name of the Redshift cluster to connect to.
   * Used only by JDBC driver internally only.
   */
  CLUSTER_IDENTIFIER("clusterid",
  									null,
  									"The name of the Redshift cluster to connect to"),
  
  /**
   * The length of time (in seconds) until the temporary IAM credentials expire.
   */
  IAM_DURATION("iamduration",
  								null,
  							"The length of time (in seconds) until the temporary IAM credentials expire."),
  
  /**
   * The IAM access key id for the IAM user or role.
   */
  IAM_ACCESS_KEY_ID("accesskeyid",
  									null,
  									"The IAM access key id for the IAM user or role"),

  /**
   * Indicates whether use IAM authentication.
   * Used only by JDBC driver internally only.
   */
  IAM_AUTH("iamauth",
  				 "false",
  				 "Indicates whether use IAM authentication"),

  /**
   * Disable IAM credentials cache.
   * Enable cache gives protection against throttling API calls.
   * Default value is false.
   */
  IAM_DISABLE_CACHE("iamdisablecache",
  				 "false",
  				 "Indicates to disable credential cache. Enable cache gives protection against throttling API calls"),

  /**
   * Indicates the display name to be used for the client that is using IdC browser auth plugin.
   */
  IDC_CLIENT_DISPLAY_NAME("idc_client_display_name",
          "Amazon Redshift JDBC driver",
           "Indicates the display name to be used for the client that is using IdC browser auth plugin"),

  /**
   * Indicates the identity namespace to be used for connection.
   * It is used by Redshift server to decide which IdC instance to use.
   */
  IDC_IDENTITY_NAMESPACE("identity_namespace",
          null,
          "Indicates the identity namespace which helps to decide which IdC instance to use in Redshift server"),

  /**
   * Indicates the token type to be used for connection.
   * It is an optional param used in the CommonCredentialsProvider plugin to allow the client to set token type
   */
  TOKEN_TYPE("token_type",
          null,
          "Indicates the optional token type to be used for connection"),

  /**
   * The AWS region where the cluster is located.
   * Used only by JDBC driver internally only.
   */
  AWS_REGION("region",
  					 null,
  					 "The AWS region where the cluster is located"),

  /**
   * The Redshift endpoint url.
   * Used only AWS internal team.
   */
  ENDPOINT_URL("endpointurl",
  							null,
  							"The Redshift endpoint url"),

  /**
   * The STS endpoint url.
   */
  STS_ENDPOINT_URL("stsendpointurl",
  							null,
  							"The STS endpoint url"),
  
  /**
   * The AWS profile name for credentials.
   */
  AWS_PROFILE("profile",
  						null,
  						"The AWS profile name for credentials"),

  /**
   * The IAM secret key for the IAM user or role.
   */
  IAM_SECRET_ACCESS_KEY("secretaccesskey",
  											null,
  											"The IAM secret key for the IAM user or role"),
 

  /**
   * The IAM security token for an IAM user or role.
   */
  IAM_SESSION_TOKEN("sessiontoken",
  									null,
  									"The IAM security token for an IAM user or role"),
  
  /**
   * The fully qualified class path for a class that implements AWSCredentialsProvider.
   */
  CREDENTIALS_PROVIDER("plugin_name",
  										 null,
  										 "The fully qualified class path for a class that implements AWSCredentialsProvider"),
  
  /**
   * Indicates whether the user should be created if not exists.
   */
  USER_AUTOCREATE("autocreate",
  								null,
  								"Indicates whether the user should be created if not exists"),
  									
  /**
   * The database user name.
   */
  DB_USER("dbuser",
  				null,
  				"The database user name"),

  /**
   * A comma delimited database group names.
   */
  DB_GROUPS("dbgroups",
  					null,
  					"A comma delimited database group names"),
  /**
   * Regex for filtering out dbGroups.
   */
  DB_GROUPS_FILTER("dbgroupsfilter",
  					null,
  					"Regex for filtering out dbGroups from final result"),
  
  /**
   * Forces database group names to be lower case.
   */
  FORCE_LOWERCASE("forcelowercase",
  					null,
  					"Forces database group names to be lower case"),
  
  /**
   * Use the IDP Groups in the Redshift.
   * This is supported by new GetClusterCredentialsV2 API.
   * Default value is false for backward compatibility, which uses
   * STS API and GetClusterCredentials for user federation and explictily
   * specifying DbGroups in connection.
   */
  GROUP_FEDERATION("groupfederation",
  					"false",
  					"Use the IDP Groups in the Redshift"),
  
  /**
   * The Oauth access token for an idp connection.
   */
  WEB_IDENTITY_TOKEN("webidentitytoken",
           null,
           "The Oauth access token for an idp connection"),
  

  /**
   * The name of the Redshift Native Auth Provider.
   */
  PROVIDER_NAME("providername",
           null,
           "The name of the Redshift Native Auth Provider"),
  
  /**
   * Set true when end point host is for serverless.
   * Driver auto detect from the given host.
   * For NLB, it won't so user can set explicitly.
   * Default value is false.
   */
  IS_SERVERLESS("isserverless",
                    "false",
                    "Redshift end-point is serverless or provisional."),
  
  /**
   * The account ID of the serverless.
   * Driver auto detect from the given host.
   * For NLB, it won't so user can set explicitly.
   * Default value is null.
   * 
   */
  SERVERLESS_ACCT_ID("serverlessacctid",
           null,
           "The account ID of the serverless"),
  
  /**
   * The work group of the serverless.
   * Driver auto detect from the given host.
   * For NLB, it won't so user can set explicitly.
   * Default value is null.
   * 
   */
  SERVERLESS_WORK_GROUP("serverlessworkgroup",
           null,
           "The work group of the serverless"),
  
  
  ;
  
  private final String name;
  private final String defaultValue;
  private final boolean required;
  private final String description;
  private final String[] choices;
  private final boolean deprecated;

  private static final Set<String> publicProperties = Collections.unmodifiableSet( new HashSet<>(Arrays.asList
          (
                   "AccessKeyID"
                  ,"AllowDBUserOverride"
                  ,"App_ID"
                  ,"App_Name"
                  ,"ApplicationName"
                  ,"AuthProfile"
                  ,"AutoCreate"
                  ,"Client_ID"
                  ,"Client_Secret"
                  ,"client_protocol_version"
                  ,"ClusterID"
                  ,"connectTimeout"
                  ,"databaseMetadataCurrentDbOnly"
                  ,"DbUser"
                  ,"DbGroups"
                  ,"DBNAME"
                  ,"defaultRowFetchSize"
                  ,"DisableIsValidQuery"
                  ,"enableFetchReadAndProcessBuffers"
                  ,"enableFetchRingBuffer"
                  ,"enableMultiSqlSupport"
                  ,"endpointUrl"
                  ,"fetchRingBufferSize"
                  ,"ForceLowercase"
                  ,"groupFederation"
                  ,"HOST"
                  ,"IAMDisableCache"
                  ,"IAMDuration"
                  ,"IdP_Host"
                  ,"IdP_Port"
                  ,"IdP_Tenant"
                  ,"IdP_Response_Timeout"
                  ,"IniFile"
                  ,"IniSection"
                  ,"isServerless"
                  ,"Login_URL"
                  ,"loginTimeout"
                  ,"loginToRp"
                  ,"LogLevel"
                  ,"LogPath"
                  ,"OverrideSchemaPatternType"
                  ,"Partner_SPID"
                  ,"Password"
                  ,"Plugin_Name"
                  ,"PORT"
                  ,"Preferred_Role"
                  ,"Profile"
                  ,"PWD"
                  ,"queryGroup"
                  ,"readOnly"
                  ,"Region"
                  ,"reWriteBatchedInserts"
                  ,"reWriteBatchedInsertsSize"
                  ,"roleArn"
                  ,"roleSessionName"
                  ,"scope"
                  ,"SecretAccessKey"
                  ,"SessionToken"
                  ,"serverlessAcctId"
                  ,"serverlessWorkGroup"
                  ,"socketFactory"
                  ,"socketTimeout"
                  ,"SSL"
                  ,"SSL_Insecure"
                  ,"SSLCert"
                  ,"SSLFactory"
                  ,"SSLKey"
                  ,"SSLMode"
                  ,"SSLPassword"
                  ,"SSLRootCert"
                  ,"StsEndpointUrl"
                  ,"tcpKeepAlive"
                  ,"UID"
                  ,"User"
                  ,"Username"
                  ,"webIdentityToken"
          )
  ));

  RedshiftProperty(String name, String defaultValue, String description) {
    this(name, defaultValue, description, false);
  }

  RedshiftProperty(String name, String defaultValue, String description, boolean required) {
    this(name, defaultValue, description, required, (String[]) null);
  }

  RedshiftProperty(String name, String defaultValue, String description, boolean required, String[] choices) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.required = required;
    this.description = description;
    this.choices = choices;
    try {
      this.deprecated = RedshiftProperty.class.getField(name()).getAnnotation(Deprecated.class) != null;
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Map<String, RedshiftProperty> PROPS_BY_NAME = new HashMap<String, RedshiftProperty>();
  static {
    for (RedshiftProperty prop : RedshiftProperty.values()) {
      if (PROPS_BY_NAME.put(prop.getName(), prop) != null) {
        throw new IllegalStateException("Duplicate RedshiftProperty name: " + prop.getName());
      }
    }
  }

  /**
   * Returns the name of the connection parameter. The name is the key that must be used in JDBC URL
   * or in Driver properties
   *
   * @return the name of the connection parameter
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the default value for this connection parameter.
   *
   * @return the default value for this connection parameter or null
   */
  public String getDefaultValue() {
    return defaultValue;
  }

  /**
   * Returns whether this parameter is required.
   *
   * @return whether this parameter is required
   */
  public boolean isRequired() {
    return required;
  }

  /**
   * Returns the description for this connection parameter.
   *
   * @return the description for this connection parameter
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns the available values for this connection parameter.
   *
   * @return the available values for this connection parameter or null
   */
  public String[] getChoices() {
    return choices;
  }

  /**
   * Returns whether this connection parameter is deprecated.
   *
   * @return whether this connection parameter is deprecated
   */
  public boolean isDeprecated() {
    return deprecated;
  }

  /**
   * Returns the value of the connection parameters according to the given {@code Properties} or the
   * default value.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter
   */
  public String get(Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  /**
   * Set the value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties in which the value should be set
   * @param value value for this connection parameter
   */
  public void set(Properties properties, String value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.setProperty(name, value);
    }
  }

  /**
   * Return the boolean value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to boolean
   */
  public boolean getBoolean(Properties properties) {
    return Boolean.valueOf(get(properties));
  }

  /**
   * Return the int value for this connection parameter in the given {@code Properties}. Prefer the
   * use of {@link #getInt(Properties)} anywhere you can throw an {@link java.sql.SQLException}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to int
   * @throws NumberFormatException if it cannot be converted to int.
   */
  public int getIntNoCheck(Properties properties) {
    String value = get(properties);
    return Integer.parseInt(value);
  }

  /**
   * Return the int value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to int
   * @throws RedshiftException if it cannot be converted to int.
   */
  public int getInt(Properties properties) throws RedshiftException {
    String value = get(properties);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      throw new RedshiftException(GT.tr("{0} parameter value must be an integer but was: {1}",
          getName(), value), RedshiftState.INVALID_PARAMETER_VALUE, nfe);
    }
  }

  /**
   * Return the {@code Integer} value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties to take actual value from
   * @return evaluated value for this connection parameter converted to Integer or null
   * @throws RedshiftException if unable to parse property as integer
   */
  public Integer getInteger(Properties properties) throws RedshiftException {
    String value = get(properties);
    if (value == null) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      throw new RedshiftException(GT.tr("{0} parameter value must be an integer but was: {1}",
          getName(), value), RedshiftState.INVALID_PARAMETER_VALUE, nfe);
    }
  }

  /**
   * Set the boolean value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties in which the value should be set
   * @param value boolean value for this connection parameter
   */
  public void set(Properties properties, boolean value) {
    properties.setProperty(name, Boolean.toString(value));
  }

  /**
   * Set the int value for this connection parameter in the given {@code Properties}.
   *
   * @param properties properties in which the value should be set
   * @param value int value for this connection parameter
   */
  public void set(Properties properties, int value) {
    properties.setProperty(name, Integer.toString(value));
  }

  /**
   * Test whether this property is present in the given {@code Properties}.
   *
   * @param properties set of properties to check current in
   * @return true if the parameter is specified in the given properties
   */
  public boolean isPresent(Properties properties) {
    return getSetString(properties) != null;
  }

  /**
   * Convert this connection parameter and the value read from the given {@code Properties} into a
   * {@code DriverPropertyInfo}.
   *
   * @param properties properties to take actual value from
   * @return a DriverPropertyInfo representing this connection parameter
   */
  public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
    DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
    propertyInfo.required = required;
    propertyInfo.description = description;
    propertyInfo.choices = choices;
    return propertyInfo;
  }

  public static RedshiftProperty forName(String name) {
    return PROPS_BY_NAME.get(name);
  }

  /**
   * Return the property if exists but avoiding the default. Allowing the caller to detect the lack
   * of a property.
   *
   * @param properties properties bundle
   * @return the value of a set property
   */
  public String getSetString(Properties properties) {
    Object o = properties.get(name);
    if (o instanceof String) {
      return (String) o;
    }
    return null;
  }

  /**
   * Return the public property
   *
   * @return the value of a set property
   */
  public static Set<String> getPublicProperties()
  {
    return publicProperties;
  }
}

