/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ds.common;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.AutoSave;
import com.amazon.redshift.jdbc.PreferQueryMode;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.ExpressionProperties;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.URLCoder;
import com.amazon.redshift.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.CommonDataSource;

/**
 * Base class for data sources and related classes.
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */

public abstract class BaseDataSource implements CommonDataSource, Referenceable {

  // Standard properties, defined in the JDBC 2.0 Optional Package spec
  private String[] serverNames = new String[] {"localhost"};
  private String databaseName = "";
  private String user;
  private String password;
  private int[] portNumbers = new int[] {0};

  // Map for all other properties
  private Properties properties = new Properties();

  /*
   * Ensure the driver is loaded as JDBC Driver might be invisible to Java's ServiceLoader.
   * Usually, {@code Class.forName(...)} is not required as {@link DriverManager} detects JDBC drivers
   * via {@code META-INF/services/java.sql.Driver} entries. However there might be cases when the driver
   * is located at the application level classloader, thus it might be required to perform manual
   * registration of the driver.
   */
  static {
    try {
      Class.forName("com.amazon.redshift.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
        "BaseDataSource is unable to load com.amazon.redshift.Driver. Please check if you have proper Redshift JDBC Driver jar on the classpath",
        e);
    }
  }

  /**
   * Gets a connection to the Redshift database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the DataSource properties user and password.
   *
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public Connection getConnection() throws SQLException {
    return getConnection(user, password);
  }

  /**
   * Gets a connection to the Redshift database. The database is identified by the DataSource
   * properties serverName, databaseName, and portNumber. The user to connect as is identified by
   * the arguments user and password, which override the DataSource properties by the same name.
   *
   * @param user     user
   * @param password password
   * @return A valid database connection.
   * @throws SQLException Occurs when the database connection cannot be established.
   */
  public Connection getConnection(String user, String password) throws SQLException {
    try {
      Connection con = DriverManager.getConnection(getUrl(), user, password);
      if (RedshiftLogger.isEnable()) {
        ((BaseConnection)con).getLogger().log(LogLevel.DEBUG, "Created a {0} for {1} at {2}",
            new Object[] {getDescription(), user, RedshiftLogger.maskSecureInfoInUrl(getUrl())});
      }
      
      if (RedshiftLogger.isEnable()) 
        ((BaseConnection)con).getLogger().logFunction(false, con); 
      
      return con;
      
    } catch (SQLException e) {
    	if (RedshiftLogger.isEnable())
    		RedshiftLogger.getDriverLogger().logError(e);
      throw e;
    }
  }

  /**
   * This implementation don't use a LogWriter.
   */
  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  /**
   * This implementation don't use a LogWriter.
   *
   * @param printWriter Not used
   */
  @Override
  public void setLogWriter(PrintWriter printWriter) {
    // NOOP
  }

  /**
   * Gets the name of the host the Redshift database is running on.
   *
   * @return name of the host the Redshift database is running on
   * @deprecated use {@link #getServerNames()}
   */
  @Deprecated
  public String getServerName() {
    return serverNames[0];
  }

  /**
   * Gets the name of the host(s) the Redshift database is running on.
   *
   * @return name of the host(s) the Redshift database is running on
   */
  public String[] getServerNames() {
    return serverNames;
  }

  /**
   * Sets the name of the host the Redshift database is running on. If this is changed, it will
   * only affect future calls to getConnection. The default value is {@code localhost}.
   *
   * @param serverName name of the host the Redshift database is running on
   * @deprecated use {@link #setServerNames(String[])}
   */
  @Deprecated
  public void setServerName(String serverName) {
    this.setServerNames(new String[] { serverName });
  }

  /**
   * Sets the name of the host(s) the Redshift database is running on. If this is changed, it will
   * only affect future calls to getConnection. The default value is {@code localhost}.
   *
   * @param serverNames name of the host(s) the Redshift database is running on
   */
  public void setServerNames(String[] serverNames) {
    if (serverNames == null || serverNames.length == 0) {
      this.serverNames = new String[] {"localhost"};
    } else {
      serverNames = Arrays.copyOf(serverNames, serverNames.length);
      for (int i = 0; i < serverNames.length; i++) {
        if (serverNames[i] == null || serverNames[i].equals("")) {
          serverNames[i] = "localhost";
        }
      }
      this.serverNames = serverNames;
    }
  }

  /**
   * Gets the name of the Redshift database, running on the server identified by the serverName
   * property.
   *
   * @return name of the Redshift database
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Sets the name of the Redshift database, running on the server identified by the serverName
   * property. If this is changed, it will only affect future calls to getConnection.
   *
   * @param databaseName name of the Redshift database
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Gets a description of this DataSource-ish thing. Must be customized by subclasses.
   *
   * @return description of this DataSource-ish thing
   */
  public abstract String getDescription();

  /**
   * Gets the user to connect as by default. If this is not specified, you must use the
   * getConnection method which takes a user and password as parameters.
   *
   * @return user to connect as by default
   */
  public String getUser() {
    return user;
  }
  
  /**
   * Added for backward compatibility.
   * 
   * @return
   */
  public String getUserID() {
  	return getUser();
  }
  

  /**
   * Sets the user to connect as by default. If this is not specified, you must use the
   * getConnection method which takes a user and password as parameters. If this is changed, it will
   * only affect future calls to getConnection.
   *
   * @param user user to connect as by default
   */
  public void setUser(String user) {
    this.user = user;
  }
  
  /**
   * Added for backward compatibility.
   * 
   * @param id
   */
  public void setUserID(String id) {
  	setUser(id);
  }
  

  /**
   * Gets the password to connect with by default. If this is not specified but a password is needed
   * to log in, you must use the getConnection method which takes a user and password as parameters.
   *
   * @return password to connect with by default
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password to connect with by default. If this is not specified but a password is needed
   * to log in, you must use the getConnection method which takes a user and password as parameters.
   * If this is changed, it will only affect future calls to getConnection.
   *
   * @param password password to connect with by default
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets the port which the Redshift server is listening on for TCP/IP connections.
   *
   * @return The port, or 0 if the default port will be used.
   * @deprecated use {@link #getPortNumbers()}
   */
  @Deprecated
  public int getPortNumber() {
    if (portNumbers == null || portNumbers.length == 0) {
      return 0;
    }
    return portNumbers[0];
  }

  /**
   * Gets the port(s) which the Redshift server is listening on for TCP/IP connections.
   *
   * @return The port(s), or 0 if the default port will be used.
   */
  public int[] getPortNumbers() {
    return portNumbers;
  }

  /**
   * Sets the port which the Redshift server is listening on for TCP/IP connections. Be sure the
   * -i flag is passed to postmaster when Redshift is started. If this is not set, or set to 0,
   * the default port will be used.
   *
   * @param portNumber port which the Redshift server is listening on for TCP/IP
   * @deprecated use {@link #setPortNumbers(int[])}
   */
  @Deprecated
  public void setPortNumber(int portNumber) {
    setPortNumbers(new int[] { portNumber });
  }

  /**
   * Sets the port(s) which the Redshift server is listening on for TCP/IP connections. Be sure the
   * -i flag is passed to postmaster when Redshift is started. If this is not set, or set to 0,
   * the default port will be used.
   *
   * @param portNumbers port(s) which the Redshift server is listening on for TCP/IP
   */
  public void setPortNumbers(int[] portNumbers) {
    if (portNumbers == null || portNumbers.length == 0) {
      portNumbers = new int[] { 0 };
    }
    this.portNumbers = Arrays.copyOf(portNumbers, portNumbers.length);
  }

  /**
   * @return command line options for this connection
   */
  public String getOptions() {
    return RedshiftProperty.OPTIONS.get(properties);
  }

  /**
   * Set command line options for this connection
   *
   * @param options string to set options to
   */
  public void setOptions(String options) {
    RedshiftProperty.OPTIONS.set(properties, options);
  }

  /**
   * @return login timeout
   * @see RedshiftProperty#LOGIN_TIMEOUT
   */
  @Override
  public int getLoginTimeout() {
    return RedshiftProperty.LOGIN_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param loginTimeout login timeout
   * @see RedshiftProperty#LOGIN_TIMEOUT
   */
  @Override
  public void setLoginTimeout(int loginTimeout) {
    RedshiftProperty.LOGIN_TIMEOUT.set(properties, loginTimeout);
  }

  /**
   * @return connect timeout
   * @see RedshiftProperty#CONNECT_TIMEOUT
   */
  public int getConnectTimeout() {
    return RedshiftProperty.CONNECT_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param connectTimeout connect timeout
   * @see RedshiftProperty#CONNECT_TIMEOUT
   */
  public void setConnectTimeout(int connectTimeout) {
    RedshiftProperty.CONNECT_TIMEOUT.set(properties, connectTimeout);
  }

  /**
   * @return protocol version
   * @see RedshiftProperty#PROTOCOL_VERSION
   */
  public int getProtocolVersion() {
    if (!RedshiftProperty.PROTOCOL_VERSION.isPresent(properties)) {
      return 0;
    } else {
      return RedshiftProperty.PROTOCOL_VERSION.getIntNoCheck(properties);
    }
  }

  /**
   * @param protocolVersion protocol version
   * @see RedshiftProperty#PROTOCOL_VERSION
   */
  public void setProtocolVersion(int protocolVersion) {
    if (protocolVersion == 0) {
      RedshiftProperty.PROTOCOL_VERSION.set(properties, null);
    } else {
      RedshiftProperty.PROTOCOL_VERSION.set(properties, protocolVersion);
    }
  }

  /**
   * @return receive buffer size
   * @see RedshiftProperty#RECEIVE_BUFFER_SIZE
   */
  public int getReceiveBufferSize() {
    return RedshiftProperty.RECEIVE_BUFFER_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param nbytes receive buffer size
   * @see RedshiftProperty#RECEIVE_BUFFER_SIZE
   */
  public void setReceiveBufferSize(int nbytes) {
    RedshiftProperty.RECEIVE_BUFFER_SIZE.set(properties, nbytes);
  }

  /**
   * @return send buffer size
   * @see RedshiftProperty#SEND_BUFFER_SIZE
   */
  public int getSendBufferSize() {
    return RedshiftProperty.SEND_BUFFER_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param nbytes send buffer size
   * @see RedshiftProperty#SEND_BUFFER_SIZE
   */
  public void setSendBufferSize(int nbytes) {
    RedshiftProperty.SEND_BUFFER_SIZE.set(properties, nbytes);
  }

  /**
   * @param count prepare threshold
   * @see RedshiftProperty#PREPARE_THRESHOLD
   */
  public void setPrepareThreshold(int count) {
    RedshiftProperty.PREPARE_THRESHOLD.set(properties, count);
  }

  /**
   * @return prepare threshold
   * @see RedshiftProperty#PREPARE_THRESHOLD
   */
  public int getPrepareThreshold() {
    return RedshiftProperty.PREPARE_THRESHOLD.getIntNoCheck(properties);
  }

  /**
   * @return prepared statement cache size (number of statements per connection)
   * @see RedshiftProperty#PREPARED_STATEMENT_CACHE_QUERIES
   */
  public int getPreparedStatementCacheQueries() {
    return RedshiftProperty.PREPARED_STATEMENT_CACHE_QUERIES.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize prepared statement cache size (number of statements per connection)
   * @see RedshiftProperty#PREPARED_STATEMENT_CACHE_QUERIES
   */
  public void setPreparedStatementCacheQueries(int cacheSize) {
    RedshiftProperty.PREPARED_STATEMENT_CACHE_QUERIES.set(properties, cacheSize);
  }

  /**
   * @return prepared statement cache size (number of megabytes per connection)
   * @see RedshiftProperty#PREPARED_STATEMENT_CACHE_SIZE_MIB
   */
  public int getPreparedStatementCacheSizeMiB() {
    return RedshiftProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize statement cache size (number of megabytes per connection)
   * @see RedshiftProperty#PREPARED_STATEMENT_CACHE_SIZE_MIB
   */
  public void setPreparedStatementCacheSizeMiB(int cacheSize) {
    RedshiftProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.set(properties, cacheSize);
  }

  /**
   * @return database metadata cache fields size (number of fields cached per connection)
   * @see RedshiftProperty#DATABASE_METADATA_CACHE_FIELDS
   */
  public int getDatabaseMetadataCacheFields() {
    return RedshiftProperty.DATABASE_METADATA_CACHE_FIELDS.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize database metadata cache fields size (number of fields cached per connection)
   * @see RedshiftProperty#DATABASE_METADATA_CACHE_FIELDS
   */
  public void setDatabaseMetadataCacheFields(int cacheSize) {
    RedshiftProperty.DATABASE_METADATA_CACHE_FIELDS.set(properties, cacheSize);
  }

  /**
   * @return database metadata cache fields size (number of megabytes per connection)
   * @see RedshiftProperty#DATABASE_METADATA_CACHE_FIELDS_MIB
   */
  public int getDatabaseMetadataCacheFieldsMiB() {
    return RedshiftProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.getIntNoCheck(properties);
  }

  /**
   * @param cacheSize database metadata cache fields size (number of megabytes per connection)
   * @see RedshiftProperty#DATABASE_METADATA_CACHE_FIELDS_MIB
   */
  public void setDatabaseMetadataCacheFieldsMiB(int cacheSize) {
    RedshiftProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.set(properties, cacheSize);
  }

  /**
   * @param fetchSize default fetch size
   * @see RedshiftProperty#DEFAULT_ROW_FETCH_SIZE
   */
  public void setDefaultRowFetchSize(int fetchSize) {
    RedshiftProperty.DEFAULT_ROW_FETCH_SIZE.set(properties, fetchSize);
  }

  /**
   * @return default fetch size
   * @see RedshiftProperty#DEFAULT_ROW_FETCH_SIZE
   */
  public int getDefaultRowFetchSize() {
    return RedshiftProperty.DEFAULT_ROW_FETCH_SIZE.getIntNoCheck(properties);
  }

  /**
   * @param unknownLength unknown length
   * @see RedshiftProperty#UNKNOWN_LENGTH
   */
  public void setUnknownLength(int unknownLength) {
    RedshiftProperty.UNKNOWN_LENGTH.set(properties, unknownLength);
  }

  /**
   * @return unknown length
   * @see RedshiftProperty#UNKNOWN_LENGTH
   */
  public int getUnknownLength() {
    return RedshiftProperty.UNKNOWN_LENGTH.getIntNoCheck(properties);
  }

  /**
   * @param seconds socket timeout
   * @see RedshiftProperty#SOCKET_TIMEOUT
   */
  public void setSocketTimeout(int seconds) {
    RedshiftProperty.SOCKET_TIMEOUT.set(properties, seconds);
  }

  /**
   * @return socket timeout
   * @see RedshiftProperty#SOCKET_TIMEOUT
   */
  public int getSocketTimeout() {
    return RedshiftProperty.SOCKET_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param seconds timeout that is used for sending cancel command
   * @see RedshiftProperty#CANCEL_SIGNAL_TIMEOUT
   */
  public void setCancelSignalTimeout(int seconds) {
    RedshiftProperty.CANCEL_SIGNAL_TIMEOUT.set(properties, seconds);
  }

  /**
   * @return timeout that is used for sending cancel command in seconds
   * @see RedshiftProperty#CANCEL_SIGNAL_TIMEOUT
   */
  public int getCancelSignalTimeout() {
    return RedshiftProperty.CANCEL_SIGNAL_TIMEOUT.getIntNoCheck(properties);
  }

  /**
   * @param enabled if SSL is enabled
   * @see RedshiftProperty#SSL
   */
  public void setSsl(boolean enabled) {
    if (enabled) {
      RedshiftProperty.SSL.set(properties, true);
    } else {
      RedshiftProperty.SSL.set(properties, false);
    }
  }

  /**
   * @return true if SSL is enabled
   * @see RedshiftProperty#SSL
   */
  public boolean getSsl() {
    // "true" if "ssl" is set but empty
    return RedshiftProperty.SSL.getBoolean(properties) || "".equals(RedshiftProperty.SSL.get(properties));
  }

  /**
   * @param classname SSL factory class name
   * @see RedshiftProperty#SSL_FACTORY
   */
  public void setSslfactory(String classname) {
    RedshiftProperty.SSL_FACTORY.set(properties, classname);
  }

  /**
   * @return SSL factory class name
   * @see RedshiftProperty#SSL_FACTORY
   */
  public String getSslfactory() {
    return RedshiftProperty.SSL_FACTORY.get(properties);
  }

  /**
   * @return SSL mode
   * @see RedshiftProperty#SSL_MODE
   */
  public String getSslMode() {
    return RedshiftProperty.SSL_MODE.get(properties);
  }

  /**
   * @param mode SSL mode
   * @see RedshiftProperty#SSL_MODE
   */
  public void setSslMode(String mode) {
    RedshiftProperty.SSL_MODE.set(properties, mode);
  }

  /**
   * @return SSL mode
   * @see RedshiftProperty#SSL_FACTORY_ARG
   */
  public String getSslFactoryArg() {
    return RedshiftProperty.SSL_FACTORY_ARG.get(properties);
  }

  /**
   * @param arg argument forwarded to SSL factory
   * @see RedshiftProperty#SSL_FACTORY_ARG
   */
  public void setSslFactoryArg(String arg) {
    RedshiftProperty.SSL_FACTORY_ARG.set(properties, arg);
  }

  /**
   * @return argument forwarded to SSL factory
   * @see RedshiftProperty#SSL_HOSTNAME_VERIFIER
   */
  public String getSslHostnameVerifier() {
    return RedshiftProperty.SSL_HOSTNAME_VERIFIER.get(properties);
  }

  /**
   * @param className SSL hostname verifier
   * @see RedshiftProperty#SSL_HOSTNAME_VERIFIER
   */
  public void setSslHostnameVerifier(String className) {
    RedshiftProperty.SSL_HOSTNAME_VERIFIER.set(properties, className);
  }

  /**
   * @return className SSL hostname verifier
   * @see RedshiftProperty#SSL_CERT
   */
  public String getSslCert() {
    return RedshiftProperty.SSL_CERT.get(properties);
  }

  /**
   * @param file SSL certificate
   * @see RedshiftProperty#SSL_CERT
   */
  public void setSslCert(String file) {
    RedshiftProperty.SSL_CERT.set(properties, file);
  }

  /**
   * @return SSL certificate
   * @see RedshiftProperty#SSL_KEY
   */
  public String getSslKey() {
    return RedshiftProperty.SSL_KEY.get(properties);
  }

  /**
   * @param file SSL key
   * @see RedshiftProperty#SSL_KEY
   */
  public void setSslKey(String file) {
    RedshiftProperty.SSL_KEY.set(properties, file);
  }

  /**
   * @return SSL root certificate
   * @see RedshiftProperty#SSL_ROOT_CERT
   */
  public String getSslRootCert() {
    return RedshiftProperty.SSL_ROOT_CERT.get(properties);
  }

  /**
   * @param file SSL root certificate
   * @see RedshiftProperty#SSL_ROOT_CERT
   */
  public void setSslRootCert(String file) {
    RedshiftProperty.SSL_ROOT_CERT.set(properties, file);
  }

  /**
   * @return SSL password
   * @see RedshiftProperty#SSL_PASSWORD
   */
  public String getSslPassword() {
    return RedshiftProperty.SSL_PASSWORD.get(properties);
  }

  /**
   * @param password SSL password
   * @see RedshiftProperty#SSL_PASSWORD
   */
  public void setSslPassword(String password) {
    RedshiftProperty.SSL_PASSWORD.set(properties, password);
  }

  /**
   * @return SSL password callback
   * @see RedshiftProperty#SSL_PASSWORD_CALLBACK
   */
  public String getSslPasswordCallback() {
    return RedshiftProperty.SSL_PASSWORD_CALLBACK.get(properties);
  }

  /**
   * @param className SSL password callback class name
   * @see RedshiftProperty#SSL_PASSWORD_CALLBACK
   */
  public void setSslPasswordCallback(String className) {
    RedshiftProperty.SSL_PASSWORD_CALLBACK.set(properties, className);
  }

  /**
   * @param applicationName application name
   * @see RedshiftProperty#APPLICATION_NAME
   */
  public void setApplicationName(String applicationName) {
    RedshiftProperty.APPLICATION_NAME.set(properties, applicationName);
  }

  /**
   * @return application name
   * @see RedshiftProperty#APPLICATION_NAME
   */
  public String getApplicationName() {
    return RedshiftProperty.APPLICATION_NAME.get(properties);
  }

  /**
   * @param targetServerType target server type
   * @see RedshiftProperty#TARGET_SERVER_TYPE
   */
  public void setTargetServerType(String targetServerType) {
    RedshiftProperty.TARGET_SERVER_TYPE.set(properties, targetServerType);
  }

  /**
   * @return target server type
   * @see RedshiftProperty#TARGET_SERVER_TYPE
   */
  public String getTargetServerType() {
    return RedshiftProperty.TARGET_SERVER_TYPE.get(properties);
  }

  /**
   * @param loadBalanceHosts load balance hosts
   * @see RedshiftProperty#LOAD_BALANCE_HOSTS
   */
  public void setLoadBalanceHosts(boolean loadBalanceHosts) {
    RedshiftProperty.LOAD_BALANCE_HOSTS.set(properties, loadBalanceHosts);
  }

  /**
   * @return load balance hosts
   * @see RedshiftProperty#LOAD_BALANCE_HOSTS
   */
  public boolean getLoadBalanceHosts() {
    return RedshiftProperty.LOAD_BALANCE_HOSTS.isPresent(properties);
  }

  /**
   * @param hostRecheckSeconds host recheck seconds
   * @see RedshiftProperty#HOST_RECHECK_SECONDS
   */
  public void setHostRecheckSeconds(int hostRecheckSeconds) {
    RedshiftProperty.HOST_RECHECK_SECONDS.set(properties, hostRecheckSeconds);
  }

  /**
   * @return host recheck seconds
   * @see RedshiftProperty#HOST_RECHECK_SECONDS
   */
  public int getHostRecheckSeconds() {
    return RedshiftProperty.HOST_RECHECK_SECONDS.getIntNoCheck(properties);
  }

  /**
   * @param enabled if TCP keep alive should be enabled
   * @see RedshiftProperty#TCP_KEEP_ALIVE
   */
  public void setTcpKeepAlive(boolean enabled) {
    RedshiftProperty.TCP_KEEP_ALIVE.set(properties, enabled);
  }

  /**
   * @return true if TCP keep alive is enabled
   * @see RedshiftProperty#TCP_KEEP_ALIVE
   */
  public boolean getTcpKeepAlive() {
    return RedshiftProperty.TCP_KEEP_ALIVE.getBoolean(properties);
  }

  /**
   * @param enabled if binary transfer should be enabled
   * @see RedshiftProperty#BINARY_TRANSFER
   */
  public void setBinaryTransfer(boolean enabled) {
    RedshiftProperty.BINARY_TRANSFER.set(properties, enabled);
  }

  /**
   * @return true if binary transfer is enabled
   * @see RedshiftProperty#BINARY_TRANSFER
   */
  public boolean getBinaryTransfer() {
    return RedshiftProperty.BINARY_TRANSFER.getBoolean(properties);
  }

  /**
   * @param oidList list of OIDs that are allowed to use binary transfer
   * @see RedshiftProperty#BINARY_TRANSFER_ENABLE
   */
  public void setBinaryTransferEnable(String oidList) {
    RedshiftProperty.BINARY_TRANSFER_ENABLE.set(properties, oidList);
  }

  /**
   * @return list of OIDs that are allowed to use binary transfer
   * @see RedshiftProperty#BINARY_TRANSFER_ENABLE
   */
  public String getBinaryTransferEnable() {
    return RedshiftProperty.BINARY_TRANSFER_ENABLE.get(properties);
  }

  /**
   * @param oidList list of OIDs that are not allowed to use binary transfer
   * @see RedshiftProperty#BINARY_TRANSFER_DISABLE
   */
  public void setBinaryTransferDisable(String oidList) {
    RedshiftProperty.BINARY_TRANSFER_DISABLE.set(properties, oidList);
  }

  /**
   * @return list of OIDs that are not allowed to use binary transfer
   * @see RedshiftProperty#BINARY_TRANSFER_DISABLE
   */
  public String getBinaryTransferDisable() {
    return RedshiftProperty.BINARY_TRANSFER_DISABLE.get(properties);
  }

  /**
   * @return string type
   * @see RedshiftProperty#STRING_TYPE
   */
  public String getStringType() {
    return RedshiftProperty.STRING_TYPE.get(properties);
  }

  /**
   * @param stringType string type
   * @see RedshiftProperty#STRING_TYPE
   */
  public void setStringType(String stringType) {
    RedshiftProperty.STRING_TYPE.set(properties, stringType);
  }

  /**
   * @return true if column sanitizer is disabled
   * @see RedshiftProperty#DISABLE_COLUMN_SANITISER
   */
  public boolean isColumnSanitiserDisabled() {
    return RedshiftProperty.DISABLE_COLUMN_SANITISER.getBoolean(properties);
  }

  /**
   * @return true if column sanitizer is disabled
   * @see RedshiftProperty#DISABLE_COLUMN_SANITISER
   */
  public boolean getDisableColumnSanitiser() {
    return RedshiftProperty.DISABLE_COLUMN_SANITISER.getBoolean(properties);
  }

  /**
   * @param disableColumnSanitiser if column sanitizer should be disabled
   * @see RedshiftProperty#DISABLE_COLUMN_SANITISER
   */
  public void setDisableColumnSanitiser(boolean disableColumnSanitiser) {
    RedshiftProperty.DISABLE_COLUMN_SANITISER.set(properties, disableColumnSanitiser);
  }

  /**
   * @return current schema
   * @see RedshiftProperty#CURRENT_SCHEMA
   */
  public String getCurrentSchema() {
    return RedshiftProperty.CURRENT_SCHEMA.get(properties);
  }

  /**
   * @param currentSchema current schema
   * @see RedshiftProperty#CURRENT_SCHEMA
   */
  public void setCurrentSchema(String currentSchema) {
    RedshiftProperty.CURRENT_SCHEMA.set(properties, currentSchema);
  }

  /**
   * @return true if connection is readonly
   * @see RedshiftProperty#READ_ONLY
   */
  public boolean getReadOnly() {
    return RedshiftProperty.READ_ONLY.getBoolean(properties);
  }

  /**
   * @param readOnly if connection should be readonly
   * @see RedshiftProperty#READ_ONLY
   */
  public void setReadOnly(boolean readOnly) {
    RedshiftProperty.READ_ONLY.set(properties, readOnly);
  }

  /**
   * @return The behavior when set read only
   * @see RedshiftProperty#READ_ONLY_MODE
   */
  public String getReadOnlyMode() {
    return RedshiftProperty.READ_ONLY_MODE.get(properties);
  }

  /**
   * @param mode the behavior when set read only
   * @see RedshiftProperty#READ_ONLY_MODE
   */
  public void setReadOnlyMode(String mode) {
    RedshiftProperty.READ_ONLY_MODE.set(properties, mode);
  }

  /**
   * @return true if driver should log unclosed connections
   * @see RedshiftProperty#LOG_UNCLOSED_CONNECTIONS
   */
  public boolean getLogUnclosedConnections() {
    return RedshiftProperty.LOG_UNCLOSED_CONNECTIONS.getBoolean(properties);
  }

  /**
   * @param enabled true if driver should log unclosed connections
   * @see RedshiftProperty#LOG_UNCLOSED_CONNECTIONS
   */
  public void setLogUnclosedConnections(boolean enabled) {
    RedshiftProperty.LOG_UNCLOSED_CONNECTIONS.set(properties, enabled);
  }

  /**
   * @return true if driver should log include detail in server error messages
   * @see RedshiftProperty#LOG_SERVER_ERROR_DETAIL
   */
  public boolean getLogServerErrorDetail() {
    return RedshiftProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(properties);
  }

  /**
   * @param enabled true if driver should include detail in server error messages
   * @see RedshiftProperty#LOG_SERVER_ERROR_DETAIL
   */
  public void setLogServerErrorDetail(boolean enabled) {
    RedshiftProperty.LOG_SERVER_ERROR_DETAIL.set(properties, enabled);
  }

  /**
   * @return assumed minimal server version
   * @see RedshiftProperty#ASSUME_MIN_SERVER_VERSION
   */
  public String getAssumeMinServerVersion() {
    return RedshiftProperty.ASSUME_MIN_SERVER_VERSION.get(properties);
  }

  /**
   * @param minVersion assumed minimal server version
   * @see RedshiftProperty#ASSUME_MIN_SERVER_VERSION
   */
  public void setAssumeMinServerVersion(String minVersion) {
    RedshiftProperty.ASSUME_MIN_SERVER_VERSION.set(properties, minVersion);
  }

  /**
   * @return JAAS application name
   * @see RedshiftProperty#JAAS_APPLICATION_NAME
   */
  public String getJaasApplicationName() {
    return RedshiftProperty.JAAS_APPLICATION_NAME.get(properties);
  }

  /**
   * @param name JAAS application name
   * @see RedshiftProperty#JAAS_APPLICATION_NAME
   */
  public void setJaasApplicationName(String name) {
    RedshiftProperty.JAAS_APPLICATION_NAME.set(properties, name);
  }

  /**
   * @return true if perform JAAS login before GSS authentication
   * @see RedshiftProperty#JAAS_LOGIN
   */
  public boolean getJaasLogin() {
    return RedshiftProperty.JAAS_LOGIN.getBoolean(properties);
  }

  /**
   * @param doLogin true if perform JAAS login before GSS authentication
   * @see RedshiftProperty#JAAS_LOGIN
   */
  public void setJaasLogin(boolean doLogin) {
    RedshiftProperty.JAAS_LOGIN.set(properties, doLogin);
  }

  /**
   * @return Kerberos server name
   * @see RedshiftProperty#KERBEROS_SERVER_NAME
   */
  public String getKerberosServerName() {
    return RedshiftProperty.KERBEROS_SERVER_NAME.get(properties);
  }

  /**
   * @param serverName Kerberos server name
   * @see RedshiftProperty#KERBEROS_SERVER_NAME
   */
  public void setKerberosServerName(String serverName) {
    RedshiftProperty.KERBEROS_SERVER_NAME.set(properties, serverName);
  }

  /**
   * @return true if use SPNEGO
   * @see RedshiftProperty#USE_SPNEGO
   */
  public boolean getUseSpNego() {
    return RedshiftProperty.USE_SPNEGO.getBoolean(properties);
  }

  /**
   * @param use true if use SPNEGO
   * @see RedshiftProperty#USE_SPNEGO
   */
  public void setUseSpNego(boolean use) {
    RedshiftProperty.USE_SPNEGO.set(properties, use);
  }

  /**
   * @return GSS mode: auto, sspi, or gssapi
   * @see RedshiftProperty#GSS_LIB
   */
  public String getGssLib() {
    return RedshiftProperty.GSS_LIB.get(properties);
  }

  /**
   * @param lib GSS mode: auto, sspi, or gssapi
   * @see RedshiftProperty#GSS_LIB
   */
  public void setGssLib(String lib) {
    RedshiftProperty.GSS_LIB.set(properties, lib);
  }

  /**
   * @return SSPI service class
   * @see RedshiftProperty#SSPI_SERVICE_CLASS
   */
  public String getSspiServiceClass() {
    return RedshiftProperty.SSPI_SERVICE_CLASS.get(properties);
  }

  /**
   * @param serviceClass SSPI service class
   * @see RedshiftProperty#SSPI_SERVICE_CLASS
   */
  public void setSspiServiceClass(String serviceClass) {
    RedshiftProperty.SSPI_SERVICE_CLASS.set(properties, serviceClass);
  }

  /**
   * @return if connection allows encoding changes
   * @see RedshiftProperty#ALLOW_ENCODING_CHANGES
   */
  public boolean getAllowEncodingChanges() {
    return RedshiftProperty.ALLOW_ENCODING_CHANGES.getBoolean(properties);
  }

  /**
   * @param allow if connection allows encoding changes
   * @see RedshiftProperty#ALLOW_ENCODING_CHANGES
   */
  public void setAllowEncodingChanges(boolean allow) {
    RedshiftProperty.ALLOW_ENCODING_CHANGES.set(properties, allow);
  }

  /**
   * @return socket factory class name
   * @see RedshiftProperty#SOCKET_FACTORY
   */
  public String getSocketFactory() {
    return RedshiftProperty.SOCKET_FACTORY.get(properties);
  }

  /**
   * @param socketFactoryClassName socket factory class name
   * @see RedshiftProperty#SOCKET_FACTORY
   */
  public void setSocketFactory(String socketFactoryClassName) {
    RedshiftProperty.SOCKET_FACTORY.set(properties, socketFactoryClassName);
  }

  /**
   * @return socket factory argument
   * @see RedshiftProperty#SOCKET_FACTORY_ARG
   */
  public String getSocketFactoryArg() {
    return RedshiftProperty.SOCKET_FACTORY_ARG.get(properties);
  }

  /**
   * @param socketFactoryArg socket factory argument
   * @see RedshiftProperty#SOCKET_FACTORY_ARG
   */
  public void setSocketFactoryArg(String socketFactoryArg) {
    RedshiftProperty.SOCKET_FACTORY_ARG.set(properties, socketFactoryArg);
  }

  /**
   * @param replication set to 'database' for logical replication or 'true' for physical replication
   * @see RedshiftProperty#REPLICATION
   */
  public void setReplication(String replication) {
    RedshiftProperty.REPLICATION.set(properties, replication);
  }

  /**
   * @return 'select', "callIfNoReturn', or 'call'
   * @see RedshiftProperty#ESCAPE_SYNTAX_CALL_MODE
   */
  public String getEscapeSyntaxCallMode() {
    return RedshiftProperty.ESCAPE_SYNTAX_CALL_MODE.get(properties);
  }

  /**
   * @param callMode the call mode to use for JDBC escape call syntax
   * @see RedshiftProperty#ESCAPE_SYNTAX_CALL_MODE
   */
  public void setEscapeSyntaxCallMode(String callMode) {
    RedshiftProperty.ESCAPE_SYNTAX_CALL_MODE.set(properties, callMode);
  }

  /**
   * @return null, 'database', or 'true
   * @see RedshiftProperty#REPLICATION
   */
  public String getReplication() {
    return RedshiftProperty.REPLICATION.get(properties);
  }

  /**
   * @return Logger Level of the JDBC Driver
   * @see RedshiftProperty#LOGGER_LEVEL
   */
  public String getLoggerLevel() {
    String logLevel = RedshiftProperty.LOGGER_LEVEL.get(properties);
    if (logLevel == null)
    			logLevel = RedshiftProperty.LOG_LEVEL.get(properties);
    return logLevel;
  }

  /**
   * @param loggerLevel of the JDBC Driver
   * @see RedshiftProperty#LOGGER_LEVEL
   */
  public void setLoggerLevel(String loggerLevel) {
    RedshiftProperty.LOGGER_LEVEL.set(properties, loggerLevel);
  }
  
 
  /**
   * Added for backward compatibility.
   * 
   * @param level
   */
  public void setLogLevel(String level) {
    RedshiftProperty.LOG_LEVEL.set(properties, level);
	}

  /**
   * @return File output of the Logger.
   * @see RedshiftProperty#LOGGER_FILE
   */
  public String getLoggerFile() {
    ExpressionProperties exprProps = new ExpressionProperties(properties, System.getProperties());
    String logFile = RedshiftProperty.LOGGER_FILE.get(exprProps);
    if (logFile == null) {
    	String logPath = getLogDirectory();
    	if (logPath != null)
    		logFile = logPath + File.pathSeparator + "redshift_jdbc.log";
    }
    
    return logFile;
  }
  
  public String getLogDirectory() {
    ExpressionProperties exprProps = new ExpressionProperties(properties, System.getProperties());
    return RedshiftProperty.LOG_PATH.get(exprProps);
  }
  

  /**
   * @param loggerFile File output of the Logger.
   * @see RedshiftProperty#LOGGER_LEVEL
   */
  public void setLoggerFile(String loggerFile) {
    RedshiftProperty.LOGGER_FILE.set(properties, loggerFile);
  }

  /**
   * Added for backward compatibility.
   * 
   * @param logDirectory
   */
  public void setLogDirectory(String logDirectory) {
    RedshiftProperty.LOG_PATH.set(properties, logDirectory);
  }
  
  /**
   * Generates a {@link DriverManager} URL from the other properties supplied.
   *
   * @return {@link DriverManager} URL from the other properties supplied
   */
  public String getUrl() {
    StringBuilder url = new StringBuilder(100);
    url.append("jdbc:redshift://");
    for (int i = 0; i < serverNames.length; i++) {
      if (i > 0) {
        url.append(",");
      }
      url.append(serverNames[i]);
      if (portNumbers != null && portNumbers.length >= i && portNumbers[i] != 0) {
        url.append(":").append(portNumbers[i]);
      }
    }
    url.append("/").append(URLCoder.encode(databaseName));

    StringBuilder query = new StringBuilder(100);
    for (RedshiftProperty property : RedshiftProperty.values()) {
      if (property.isPresent(properties)) {
        if (query.length() != 0) {
          query.append("&");
        }
        query.append(property.getName());
        query.append("=");
        query.append(URLCoder.encode(property.get(properties)));
      }
    }

    if (query.length() > 0) {
      url.append("?");
      url.append(query);
    }

    return url.toString();
  }

  /**
   * Generates a {@link DriverManager} URL from the other properties supplied.
   *
   * @return {@link DriverManager} URL from the other properties supplied
   */
  public String getURL() {
    return getUrl();
  }

  /**
   * Sets properties from a {@link DriverManager} URL.
   *
   * @param url properties to set
   */
  public void setUrl(String url) {

    Properties p = com.amazon.redshift.Driver.parseURL(url, null);

    if (p == null) {
      throw new IllegalArgumentException("URL invalid " + url);
    }
    for (RedshiftProperty property : RedshiftProperty.values()) {
      if (!this.properties.containsKey(property.getName())) {
        setProperty(property, property.get(p));
      }
    }
  }

  /**
   * Sets properties from a {@link DriverManager} URL.
   * Added to follow convention used in other DBMS.
   *
   * @param url properties to set
   */
  public void setURL(String url) {
    setUrl(url);
  }

  public String getProperty(String name) throws SQLException {
    RedshiftProperty pgProperty = RedshiftProperty.forName(name);
    if (pgProperty != null) {
      return getProperty(pgProperty);
    } else {
      throw new RedshiftException(GT.tr("Unsupported property name: {0}", name),
        RedshiftState.INVALID_PARAMETER_VALUE);
    }
  }

  public void setProperty(String name, String value) throws SQLException {
    RedshiftProperty pgProperty = RedshiftProperty.forName(name);
    if (pgProperty != null) {
      setProperty(pgProperty, value);
    } else {
      throw new RedshiftException(GT.tr("Unsupported property name: {0}", name),
        RedshiftState.INVALID_PARAMETER_VALUE);
    }
  }

  public String getProperty(RedshiftProperty property) {
    return property.get(properties);
  }

  public void setProperty(RedshiftProperty property, String value) {
    if (value == null) {
      return;
    }
    switch (property) {
      case HOST:
        setServerNames(value.split(","));
        break;
      case PORT:
        String[] ps = value.split(",");
        int[] ports = new int[ps.length];
        for (int i = 0 ; i < ps.length; i++) {
          try {
            ports[i] = Integer.parseInt(ps[i]);
          } catch (NumberFormatException e) {
            ports[i] = 0;
          }
        }
        setPortNumbers(ports);
        break;
      case DBNAME:
        setDatabaseName(value);
        break;
      case USER:
      case UID:
        setUser(value);
        break;
      case PASSWORD:
      case PWD:
        setPassword(value);
        break;
      default:
        properties.setProperty(property.getName(), value);
    }
  }

  /**
   * Generates a reference using the appropriate object factory.
   *
   * @return reference using the appropriate object factory
   */
  protected Reference createReference() {
    return new Reference(getClass().getName(), RedshiftObjectFactory.class.getName(), null);
  }

  public Reference getReference() throws NamingException {
    Reference ref = createReference();
    StringBuilder serverString = new StringBuilder();
    for (int i = 0; i < serverNames.length; i++) {
      if (i > 0) {
        serverString.append(",");
      }
      String serverName = serverNames[i];
      serverString.append(serverName);
    }
    ref.add(new StringRefAddr("serverName", serverString.toString()));

    StringBuilder portString = new StringBuilder();
    for (int i = 0; i < portNumbers.length; i++) {
      if (i > 0) {
        portString.append(",");
      }
      int p = portNumbers[i];
      portString.append(Integer.toString(p));
    }
    ref.add(new StringRefAddr("portNumber", portString.toString()));
    ref.add(new StringRefAddr("databaseName", databaseName));
    if (user != null) {
      ref.add(new StringRefAddr("user", user));
    }
    if (password != null) {
      ref.add(new StringRefAddr("password", password));
    }

    for (RedshiftProperty property : RedshiftProperty.values()) {
      if (property.isPresent(properties)) {
        ref.add(new StringRefAddr(property.getName(), property.get(properties)));
      }
    }

    return ref;
  }

  public void setFromReference(Reference ref) {
    databaseName = getReferenceProperty(ref, "databaseName");
    String portNumberString = getReferenceProperty(ref, "portNumber");
    if (portNumberString != null) {
      String[] ps = portNumberString.split(",");
      int[] ports = new int[ps.length];
      for (int i = 0; i < ps.length; i++) {
        try {
          ports[i] = Integer.parseInt(ps[i]);
        } catch (NumberFormatException e) {
          ports[i] = 0;
        }
      }
      setPortNumbers(ports);
    } else {
      setPortNumbers(null);
    }
    setServerNames(getReferenceProperty(ref, "serverName").split(","));

    for (RedshiftProperty property : RedshiftProperty.values()) {
      setProperty(property, getReferenceProperty(ref, property.getName()));
    }
  }

  private static String getReferenceProperty(Reference ref, String propertyName) {
    RefAddr addr = ref.get(propertyName);
    if (addr == null) {
      return null;
    }
    return (String) addr.getContent();
  }

  protected void writeBaseObject(ObjectOutputStream out) throws IOException {
    out.writeObject(serverNames);
    out.writeObject(databaseName);
    out.writeObject(user);
    out.writeObject(password);
    out.writeObject(portNumbers);

    out.writeObject(properties);
  }

  protected void readBaseObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    serverNames = (String[]) in.readObject();
    databaseName = (String) in.readObject();
    user = (String) in.readObject();
    password = (String) in.readObject();
    portNumbers = (int[]) in.readObject();

    properties = (Properties) in.readObject();
  }

  public void initializeFrom(BaseDataSource source) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    source.writeBaseObject(oos);
    oos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    readBaseObject(ois);
  }

  /**
   * @return preferred query execution mode
   * @see RedshiftProperty#PREFER_QUERY_MODE
   */
  public PreferQueryMode getPreferQueryMode() {
    return PreferQueryMode.of(RedshiftProperty.PREFER_QUERY_MODE.get(properties));
  }

  /**
   * @param preferQueryMode extended, simple, extendedForPrepared, or extendedCacheEverything
   * @see RedshiftProperty#PREFER_QUERY_MODE
   */
  public void setPreferQueryMode(PreferQueryMode preferQueryMode) {
    RedshiftProperty.PREFER_QUERY_MODE.set(properties, preferQueryMode.value());
  }

  /**
   * @return connection configuration regarding automatic per-query savepoints
   * @see RedshiftProperty#AUTOSAVE
   */
  public AutoSave getAutosave() {
    return AutoSave.of(RedshiftProperty.AUTOSAVE.get(properties));
  }

  /**
   * @param autoSave connection configuration regarding automatic per-query savepoints
   * @see RedshiftProperty#AUTOSAVE
   */
  public void setAutosave(AutoSave autoSave) {
    RedshiftProperty.AUTOSAVE.set(properties, autoSave.value());
  }

  /**
   * @return connection configuration regarding throwing exception from commit if database rolls back the transaction
   * @see RedshiftProperty#RAISE_EXCEPTION_ON_SILENT_ROLLBACK
   */
  public boolean isRaiseExceptionOnSilentRollback() {
    return RedshiftProperty.RAISE_EXCEPTION_ON_SILENT_ROLLBACK.getBoolean(properties);
  }

  /**
   * @param raiseExceptionOnSilentRollback if the database should throw exception if commit silently rolls back
   * @see RedshiftProperty#RAISE_EXCEPTION_ON_SILENT_ROLLBACK
   */
  public void setRaiseExceptionOnSilentRollback(boolean raiseExceptionOnSilentRollback) {
    RedshiftProperty.RAISE_EXCEPTION_ON_SILENT_ROLLBACK.set(properties, raiseExceptionOnSilentRollback);
  }

  /**
   * see RedshiftProperty#CLEANUP_SAVEPOINTS
   *
   * @return boolean indicating property set
   */
  public boolean getCleanupSavepoints() {
    return RedshiftProperty.CLEANUP_SAVEPOINTS.getBoolean(properties);
  }

  /**
   * see RedshiftProperty#CLEANUP_SAVEPOINTS
   *
   * @param cleanupSavepoints will cleanup savepoints after a successful transaction
   */
  public void setCleanupSavepoints(boolean cleanupSavepoints) {
    RedshiftProperty.CLEANUP_SAVEPOINTS.set(properties, cleanupSavepoints);
  }

  /**
   * @return boolean indicating property is enabled or not.
   * @see RedshiftProperty#REWRITE_BATCHED_INSERTS
   */
  public boolean getReWriteBatchedInserts() {
    return RedshiftProperty.REWRITE_BATCHED_INSERTS.getBoolean(properties);
  }

  /**
   * @param reWrite boolean value to set the property in the properties collection
   * @see RedshiftProperty#REWRITE_BATCHED_INSERTS
   */
  public void setReWriteBatchedInserts(boolean reWrite) {
    RedshiftProperty.REWRITE_BATCHED_INSERTS.set(properties, reWrite);
  }

  /**
   * @return boolean indicating property is enabled or not.
   * @see RedshiftProperty#HIDE_UNPRIVILEGED_OBJECTS
   */
  public boolean getHideUnprivilegedObjects() {
    return RedshiftProperty.HIDE_UNPRIVILEGED_OBJECTS.getBoolean(properties);
  }

  /**
   * @param hideUnprivileged boolean value to set the property in the properties collection
   * @see RedshiftProperty#HIDE_UNPRIVILEGED_OBJECTS
   */
  public void setHideUnprivilegedObjects(boolean hideUnprivileged) {
    RedshiftProperty.HIDE_UNPRIVILEGED_OBJECTS.set(properties, hideUnprivileged);
  }

  public String getMaxResultBuffer() {
    return RedshiftProperty.MAX_RESULT_BUFFER.get(properties);
  }

  public void setMaxResultBuffer(String maxResultBuffer) {
    RedshiftProperty.MAX_RESULT_BUFFER.set(properties, maxResultBuffer);
  }

  //#if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // java.util.logging.logger is not used in Redshift JDBC
    throw new SQLFeatureNotSupportedException ("java.util.logging is not used");
  } 
  //#endif

  /*
   * Alias methods below, these are to help with ease-of-use with other database tools / frameworks
   * which expect normal java bean getters / setters to exist for the property names.
   */

  public boolean isSsl() {
    return getSsl();
  }

  public String getSslfactoryarg() {
    return getSslFactoryArg();
  }

  public void setSslfactoryarg(final String arg) {
    setSslFactoryArg(arg);
  }

  public String getSslcert() {
    return getSslCert();
  }

  public void setSslcert(final String file) {
    setSslCert(file);
  }

  public String getSslmode() {
    return getSslMode();
  }

  public void setSslmode(final String mode) {
    setSslMode(mode);
  }

  public String getSslhostnameverifier() {
    return getSslHostnameVerifier();
  }

  public void setSslhostnameverifier(final String className) {
    setSslHostnameVerifier(className);
  }

  public String getSslkey() {
    return getSslKey();
  }

  public void setSslkey(final String file) {
    setSslKey(file);
  }

  public String getSslrootcert() {
    return getSslRootCert();
  }

  public void setSslrootcert(final String file) {
    setSslRootCert(file);
  }

  public String getSslpasswordcallback() {
    return getSslPasswordCallback();
  }

  public void setSslpasswordcallback(final String className) {
    setSslPasswordCallback(className);
  }

  public String getSslpassword() {
    return getSslPassword();
  }

  public void setSslpassword(final String sslpassword) {
    setSslPassword(sslpassword);
  }

  public int getRecvBufferSize() {
    return getReceiveBufferSize();
  }

  public void setRecvBufferSize(final int nbytes) {
    setReceiveBufferSize(nbytes);
  }

  public boolean isAllowEncodingChanges() {
    return getAllowEncodingChanges();
  }

  public boolean isLogUnclosedConnections() {
    return getLogUnclosedConnections();
  }

  public boolean isTcpKeepAlive() {
    return getTcpKeepAlive();
  }

  public boolean isReadOnly() {
    return getReadOnly();
  }

  public boolean isDisableColumnSanitiser() {
    return getDisableColumnSanitiser();
  }

  public boolean isLoadBalanceHosts() {
    return getLoadBalanceHosts();
  }

  public boolean isCleanupSavePoints() {
    return getCleanupSavepoints();
  }

  public void setCleanupSavePoints(final boolean cleanupSavepoints) {
    setCleanupSavepoints(cleanupSavepoints);
  }

  public boolean isReWriteBatchedInserts() {
    return getReWriteBatchedInserts();
  }
}
