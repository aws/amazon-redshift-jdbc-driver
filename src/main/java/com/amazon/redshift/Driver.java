/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.DriverInfo;
import com.amazon.redshift.util.ExpressionProperties;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.HostSpec;
import com.amazon.redshift.util.IniFile;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.SharedTimer;
import com.amazon.redshift.util.URLCoder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>The Java SQL framework allows for multiple database drivers. Each driver should supply a class
 * that implements the Driver interface</p>
 *
 * <p>The DriverManager will try to load as many drivers as it can find and then for any given
 * connection request, it will ask each driver in turn to try to connect to the target URL.</p>
 *
 * <p>It is strongly recommended that each Driver class should be small and standalone so that the
 * Driver class can be loaded and queried without bringing in vast quantities of supporting code.</p>
 *
 * <p>When a Driver class is loaded, it should create an instance of itself and register it with the
 * DriverManager. This means that a user can load and register a driver by doing
 * Class.forName("foo.bah.Driver")</p>
 *
 * @see com.amazon.redshift.RedshiftConnection
 * @see java.sql.Driver
 */
public class Driver implements java.sql.Driver {

  private static Driver registeredDriver;
  private static SharedTimer sharedTimer = new SharedTimer();
  private static final String DEFAULT_PORT =
      "5439";
  private static final String URL_PREFIX = "jdbc:redshift:";
	private static final Pattern URL_PATTERN =
			Pattern.compile("(iam:)?//([^:/?]+)(:([^/?]*))?(/([^?;]*))?([?;](.*))?");

	private static final Pattern HOST_PATTERN =
			Pattern.compile("(.+)\\.(.+)\\.(.+).redshift(-dev)?\\.amazonaws\\.com(.)*");
  
	private static RedshiftLogger logger;
  private static final String DEFAULT_INI_FILE = "rsjdbc.ini";
  private static final String DEFAULT_DRIVER_SECTION = "DRIVER";

  static {
    try {
      // moved the registerDriver from the constructor to here
      // because some clients call the driver themselves (I know, as
      // my early jdbc work did - and that was based on other examples).
      // Placing it here, means that the driver is registered once only.
      register();
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // Helper to retrieve default properties from classloader resource
  // properties files.
  private Properties defaultProperties;

  private synchronized Properties getDefaultProperties() throws IOException {
    if (defaultProperties != null) {
      return defaultProperties;
    }

    // Make sure we load properties with the maximum possible privileges.
    try {
      defaultProperties =
          AccessController.doPrivileged(new PrivilegedExceptionAction<Properties>() {
            public Properties run() throws IOException {
              return loadDefaultProperties();
            }
          });
    } catch (PrivilegedActionException e) {
      throw (IOException) e.getException();
    }

    return defaultProperties;
  }

  private Properties loadDefaultProperties() throws IOException {
    Properties merged = new Properties();

    try {
      RedshiftProperty.USER.set(merged, System.getProperty("user.name"));
    } catch (SecurityException se) {
      // We're just trying to set a default, so if we can't
      // it's not a big deal.
    }

    // If we are loaded by the bootstrap classloader, getClassLoader()
    // may return null. In that case, try to fall back to the system
    // classloader.
    //
    // We should not need to catch SecurityException here as we are
    // accessing either our own classloader, or the system classloader
    // when our classloader is null. The ClassLoader javadoc claims
    // neither case can throw SecurityException.
    ClassLoader cl = getClass().getClassLoader();
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }

    if (cl == null) {
      return merged; // Give up on finding defaults.
    }

    // When loading the driver config files we don't want settings found
    // in later files in the classpath to override settings specified in
    // earlier files. To do this we've got to read the returned
    // Enumeration into temporary storage.
    ArrayList<URL> urls = new ArrayList<URL>();
    Enumeration<URL> urlEnum = cl.getResources("com/amazon/redshift/driverconfig.properties");
    while (urlEnum.hasMoreElements()) {
      urls.add(urlEnum.nextElement());
    }

    for (int i = urls.size() - 1; i >= 0; i--) {
      URL url = urls.get(i);
      InputStream is = url.openStream();
      merged.load(is);
      is.close();
    }

    return merged;
  }

  /**
   * <p>Try to make a database connection to the given URL. The driver should return "null" if it
   * realizes it is the wrong kind of driver to connect to the given URL. This will be common, as
   * when the JDBC driverManager is asked to connect to a given URL, it passes the URL to each
   * loaded driver in turn.</p>
   *
   * <p>The driver should raise an SQLException if it is the right driver to connect to the given URL,
   * but has trouble connecting to the database.</p>
   *
   * <p>The java.util.Properties argument can be used to pass arbitrary string tag/value pairs as
   * connection arguments.</p>
   *
   * <ul>
   * <li>user - (required) The user to connect as</li>
   * <li>password - (optional) The password for the user</li>
   * <li>ssl -(optional) Use SSL when connecting to the server</li>
   * <li>readOnly - (optional) Set connection to read-only by default</li>
   * <li>charSet - (optional) The character set to be used for converting to/from
   * the database to unicode. If multibyte is enabled on the server then the character set of the
   * database is used as the default, otherwise the jvm character encoding is used as the default.
   * This value is only used when connecting to a 7.2 or older server.</li>
   * <li>loglevel - (optional) Enable logging of messages from the driver. The value is an integer
   * from 0 to 2 where: OFF = 0, INFO =1, DEBUG = 2 The output is sent to
   * DriverManager.getPrintWriter() if set, otherwise it is sent to System.out.</li>
   * <li>compatible - (optional) This is used to toggle between different functionality
   * as it changes across different releases of the jdbc driver code. The values here are versions
   * of the jdbc client and not server versions. For example in 7.1 get/setBytes worked on
   * LargeObject values, in 7.2 these methods were changed to work on bytea values. This change in
   * functionality could be disabled by setting the compatible level to be "7.1", in which case the
   * driver will revert to the 7.1 functionality.</li>
   * </ul>
   *
   * <p>Normally, at least "user" and "password" properties should be included in the properties. For a
   * list of supported character encoding , see
   * http://java.sun.com/products/jdk/1.2/docs/guide/internat/encoding.doc.html Note that you will
   * probably want to have set up the Postgres database itself to use the same encoding, with the
   * {@code -E <encoding>} argument to createdb.</p>
   *
   * <p>Our protocol takes the forms:</p>
   *
   * <pre>
   *  jdbc:redshift://host:port/database?param1=val1&amp;...
   * </pre>
   *
   * @param url the URL of the database to connect to
   * @param info a list of arbitrary tag/value pairs as connection arguments
   * @return a connection to the URL or null if it isnt us
   * @exception SQLException if a database access error occurs or the url is
   * {@code null}
   * @see java.sql.Driver#connect
   */
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    
  	if (url == null) {
      throw new SQLException("url is null");
    }
    // get defaults
    Properties defaults;

    if (!url.startsWith(URL_PREFIX)) {
      return null;
    }
    try {
      defaults = getDefaultProperties();
    } catch (IOException ioe) {
      throw new RedshiftException(GT.tr("Error loading default settings from driverconfig.properties"),
          RedshiftState.UNEXPECTED_ERROR, ioe);
    }

    // override defaults with provided properties
    Properties props = new Properties(defaults);
    
    if (info != null) {
      Set<String> e = info.stringPropertyNames();
      for (String propName : e) {
        String propValue = info.getProperty(propName);
        if (propValue == null) {
          throw new RedshiftException(
              GT.tr("Properties for the driver contains a non-string value for the key ")
                  + propName,
              RedshiftState.UNEXPECTED_ERROR);
        }
        props.setProperty(propName, propValue);
      }
    }
    
    // parse URL and add more properties
    if ((props = parseURL(url, props)) == null) {
      return null;
    }
    
    // Read INI file
    String iniFileName = getJdbcIniFile(props);
    
    if (iniFileName != null)
    	props = readJdbcIniFile(iniFileName, props);
    
    RedshiftLogger connLogger = null;
    try {
      // Setup java.util.logging.Logger using connection properties.
      connLogger = getLogger(props);

      if(RedshiftLogger.isEnable()) {
      	String temp = RedshiftLogger.maskSecureInfoInUrl(url);
      	logger.log(LogLevel.DEBUG, "===================================");
      	logger.log(LogLevel.DEBUG, "Connecting with URL: {0}", temp);
      	connLogger.log(LogLevel.DEBUG, "===================================");
      	connLogger.logFunction(true, temp, RedshiftLogger.maskSecureInfoInProps(info));
      	
      	connLogger.log(LogLevel.DEBUG, "Connecting with URL: {0}", temp);
      	if(iniFileName != null) {
      		connLogger.log(LogLevel.DEBUG, "JDBC INI FileName {0}", iniFileName);
//      		connLogger.log(LogLevel.DEBUG, "After merging JDBC INI FileName props:" + props);
      	}
      	
/*	    	String useProxyStr = System.getProperty("http.useProxy");
	    	String proxyHost = System.getProperty("https.proxyHost");
	    	String proxyPort = System.getProperty("https.proxyPort");

	    	connLogger.logDebug(
            String.format("useProxy: %s proxyHost: %s proxyPort:%s" , 
            								useProxyStr, proxyHost, proxyPort));
*/	    	
	    	
      }

      // Enforce login timeout, if specified, by running the connection
      // attempt in a separate thread. If we hit the timeout without the
      // connection completing, we abandon the connection attempt in
      // the calling thread, but the separate thread will keep trying.
      // Eventually, the separate thread will either fail or complete
      // the connection; at that point we clean up the connection if
      // we managed to establish one after all. See ConnectThread for
      // more details.
      long timeout = timeout(props);
      if (timeout <= 0) {
        Connection conn = makeConnection(url, props, connLogger);
        
        if(RedshiftLogger.isEnable())
        	connLogger.logFunction(false, conn);
        
        return conn;
      }

      ConnectThread ct = new ConnectThread(url, props, connLogger);
      Thread thread = new Thread(ct, "Redshift JDBC driver connection thread");
      thread.setDaemon(true); // Don't prevent the VM from shutting down
      thread.start();
      Connection conn = ct.getResult(timeout);
      
      if(RedshiftLogger.isEnable())
      	connLogger.logFunction(false, conn);
      
      return conn;
    } catch (RedshiftException ex1) {
    	if(RedshiftLogger.isEnable()) 
    		connLogger.logError(ex1);
    	
      // re-throw the exception, otherwise it will be caught next, and a
      // com.amazon.redshift.unusual error will be returned instead.
      throw ex1.getSQLException();
    } catch (java.security.AccessControlException ace) {
    	if(RedshiftLogger.isEnable()) 
    		connLogger.logError(ace);
    	
      throw new RedshiftException(
          GT.tr(
              "Your security policy has prevented the connection from being attempted.  You probably need to grant the connect java.net.SocketPermission to the database server host and port that you wish to connect to."),
          RedshiftState.UNEXPECTED_ERROR, ace);
    } catch (Exception ex2) {
    	if(RedshiftLogger.isEnable()) 
    		connLogger.logError(ex2);
      throw new RedshiftException(
          GT.tr(
              "Something unusual has occurred to cause the driver to fail. Please report this exception."),
          RedshiftState.UNEXPECTED_ERROR, ex2);
    }
  }

  /**
   * <p>Setup java.util.logging.Logger using connection properties.</p>
   *
   * <p>See {@link RedshiftProperty#LOGGER_FILE} and {@link RedshiftProperty#LOGGER_FILE}</p>
   *
   * @param props Connection Properties
   */
  private RedshiftLogger getLogger(final Properties props) {
    final String logLevel = RedshiftProperty.LOGGER_LEVEL.get(props);
    final String alias1LogLevel = RedshiftProperty.LOG_LEVEL.get(props);
    final String alias2LogLevel = RedshiftProperty.DSI_LOG_LEVEL.get(props);
    final String driverLogLevel = (logLevel != null) 
    															? logLevel 
    															: (alias1LogLevel != null)
    																? LogLevel.getLogLevel(alias1LogLevel).toString()
    																: (alias2LogLevel != null)
    																	? LogLevel.getLogLevel(alias2LogLevel).toString()
    																	: null;
    
    ExpressionProperties exprProps = new ExpressionProperties(props, System.getProperties());
    final String logFile = RedshiftProperty.LOGGER_FILE.get(exprProps);
    final String logPath = RedshiftProperty.LOG_PATH.get(exprProps);
    final String driverLogFile = (logFile != null) 
    																? logFile 
    																: RedshiftLogger.getLogFileUsingPath(driverLogLevel, logPath);
    String maxLogFileSize = RedshiftProperty.MAX_LOG_FILE_SIZE.get(exprProps);
    String maxLogFileCount = RedshiftProperty.MAX_LOG_FILE_COUNT.get(exprProps);
    
    // Driver logger
    if (logger == null)
    	logger = new RedshiftLogger(driverLogFile, driverLogLevel, true, maxLogFileSize, maxLogFileCount);
    
    RedshiftLogger connLogger = new RedshiftLogger(driverLogFile, driverLogLevel, false, maxLogFileSize, maxLogFileCount);
    
    return connLogger;
  }
  
  /**
   * Perform a connect in a separate thread; supports getting the results from the original thread
   * while enforcing a login timeout.
   */
  private static class ConnectThread implements Runnable {
    ConnectThread(String url, Properties props, RedshiftLogger connLogger) {
      this.url = url;
      this.props = props;
      this.connLogger = connLogger;
    }

    public void run() {
      Connection conn;
      Throwable error;

      try {
        conn = makeConnection(url, props, connLogger);
        error = null;
      } catch (Throwable t) {
        conn = null;
        error = t;
      }

      synchronized (this) {
        if (abandoned) {
          if (conn != null) {
            try {
              conn.close();
            } catch (SQLException e) {
            }
          }
        } else {
          result = conn;
          resultException = error;
          notify();
        }
      }
    }

    /**
     * Get the connection result from this (assumed running) thread. If the timeout is reached
     * without a result being available, a SQLException is thrown.
     *
     * @param timeout timeout in milliseconds
     * @return the new connection, if successful
     * @throws SQLException if a connection error occurs or the timeout is reached
     */
    public Connection getResult(long timeout) throws SQLException {
      long expiry = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + timeout;
      synchronized (this) {
        while (true) {
          if (result != null) {
            return result;
          }

          if (resultException != null) {
            if (resultException instanceof SQLException) {
              resultException.fillInStackTrace();
              throw (SQLException) resultException;
            } else {
              throw new RedshiftException(
                  GT.tr(
                      "Something unusual has occurred to cause the driver to fail. Please report this exception."),
                  RedshiftState.UNEXPECTED_ERROR, resultException);
            }
          }

          long delay = expiry - TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
          if (delay <= 0) {
            abandoned = true;
            throw new RedshiftException(GT.tr("Connection attempt timed out."),
                RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
          }

          try {
            wait(delay);
          } catch (InterruptedException ie) {

            // reset the interrupt flag
            Thread.currentThread().interrupt();
            abandoned = true;

            // throw an unchecked exception which will hopefully not be ignored by the calling code
            throw new RuntimeException(GT.tr("Interrupted while attempting to connect."));
          }
        }
      }
    }

    private final String url;
    private final Properties props;
    private Connection result;
    private Throwable resultException;
    private boolean abandoned;
    private RedshiftLogger connLogger;
  }

  /**
   * Create a connection from URL and properties. Always does the connection work in the current
   * thread without enforcing a timeout, regardless of any timeout specified in the properties.
   *
   * @param url the original URL
   * @param props the parsed/defaulted connection properties
   * @return a new connection
   * @throws SQLException if the connection could not be made
   */
  private static Connection makeConnection(String url, Properties props, RedshiftLogger logger) throws SQLException {
    return new RedshiftConnectionImpl(hostSpecs(props), user(props), database(props), props, url, logger);
  }

  /**
   * Returns true if the driver thinks it can open a connection to the given URL. Typically, drivers
   * will return true if they understand the subprotocol specified in the URL and false if they
   * don't. Our protocols start with jdbc:redshift:
   *
   * @param url the URL of the driver
   * @return true if this driver accepts the given URL
   * @see java.sql.Driver#acceptsURL
   */
  @Override
  public boolean acceptsURL(String url) {
    return parseURL(url, null) != null;
  }

  /**
   * <p>The getPropertyInfo method is intended to allow a generic GUI tool to discover what properties
   * it should prompt a human for in order to get enough information to connect to a database.</p>
   *
   * <p>Note that depending on the values the human has supplied so far, additional values may become
   * necessary, so it may be necessary to iterate through several calls to getPropertyInfo</p>
   *
   * @param url the Url of the database to connect to
   * @param info a proposed list of tag/value pairs that will be sent on connect open.
   * @return An array of DriverPropertyInfo objects describing possible properties. This array may
   *         be an empty array if no properties are required
   * @see java.sql.Driver#getPropertyInfo
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    Properties copy = new Properties(info);
    Properties parse = parseURL(url, copy);
    if (parse != null) {
      copy = parse;
    }

    RedshiftProperty[] knownProperties = RedshiftProperty.values();
    DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.length];
    for (int i = 0; i < props.length; ++i) {
      props[i] = knownProperties[i].toDriverPropertyInfo(copy);
    }

    return props;
  }

  @Override
  public int getMajorVersion() {
    return com.amazon.redshift.util.DriverInfo.MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return com.amazon.redshift.util.DriverInfo.MINOR_VERSION;
  }

  /**
   * Returns the server version series of this driver and the specific build number.
   *
   * @return JDBC driver version
   * @deprecated use {@link #getMajorVersion()} and {@link #getMinorVersion()} instead
   */
  @Deprecated
  public static String getVersion() {
    return DriverInfo.DRIVER_FULL_NAME;
  }

  /**
   * <p>Report whether the driver is a genuine JDBC compliant driver. A driver may only report "true"
   * here if it passes the JDBC compliance tests, otherwise it is required to return false. JDBC
   * compliance requires full support for the JDBC API and full support for SQL 92 Entry Level.</p>
   *
   * <p>For Redshift, this is not yet possible, as we are not SQL92 compliant (yet).</p>
   */
  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  /**
   * Constructs a new DriverURL, splitting the specified URL into its component parts.
   *
   * @param url JDBC URL to parse
   * @param defaults Default properties
   * @return Properties with elements added from the url
   */
  public static Properties parseURL(String url, Properties defaults) {
    Properties urlProps = new Properties(defaults);

    String urlServer = url;
    String urlArgs = "";
    boolean iamAuth = false;    

    int qPos = url.indexOf('?');
    if (qPos != -1) {
      urlServer = url.substring(0, qPos);
      urlArgs = url.substring(qPos + 1);
    }
    else {
    	qPos = url.indexOf(';');
      if (qPos != -1) {
        urlServer = url.substring(0, qPos);
        urlArgs = url.substring(qPos + 1);
      }
    }

    if (!urlServer.startsWith(URL_PREFIX)) {
      return null;
    }
    
    urlServer = urlServer.substring(URL_PREFIX.length());
    if (urlServer.startsWith("iam:")) {
    	String subname = urlServer;
    	// Parse the IAM URL
    	Matcher matcher = URL_PATTERN.matcher(subname);
    	if (!matcher.matches())
    	{
        // Host is a required value.
        return null;
      }
    	
      iamAuth = matcher.group(1) != null; // This must be true
      String host = matcher.group(2);
      String port = matcher.group(4);
      String schema = matcher.group(6);
      String queryString = matcher.group(8);
      
    	urlProps.setProperty(RedshiftProperty.IAM_AUTH.getName(), String.valueOf(iamAuth));

      if (null != port && !port.matches("\\d*"))
      {
        // This is new cluster_id:region type url.
      	urlProps.setProperty(RedshiftProperty.CLUSTER_IDENTIFIER.getName(), host);
      	urlProps.setProperty(RedshiftProperty.AWS_REGION.getName(), port);
      }
      else
      {
      	urlProps.setProperty(RedshiftProperty.HOST.getName(), host);
        if (null == port || port.isEmpty())
        {
            port = DEFAULT_PORT;
        }
        urlProps.setProperty(RedshiftProperty.PORT.getName(), port);

        // Trying to infer clusterID and region,
        // ClusterID and region can be override by parameters.
        Matcher m = HOST_PATTERN.matcher(host);
        if (m.matches())
        {
        	urlProps.setProperty(RedshiftProperty.CLUSTER_IDENTIFIER.getName(), m.group(1));
        	urlProps.setProperty(RedshiftProperty.AWS_REGION.getName(), m.group(3));
        }
      }
      
      if (null != schema)
      {
	      urlProps.setProperty(RedshiftProperty.DBNAME.getName(), URLCoder.decode(schema));
      }
      
      if (queryString != null)
      	urlArgs = queryString;
    } // IAM
    else {
    	urlProps.setProperty(RedshiftProperty.IAM_AUTH.getName(), String.valueOf(iamAuth));
    	
	    if (urlServer.startsWith("//")) {
	      urlServer = urlServer.substring(2);
	      int slash = urlServer.indexOf('/');
	      if (slash == -1) {
	        return null;
	      }
	      urlProps.setProperty("DBNAME", URLCoder.decode(urlServer.substring(slash + 1)));
	
	      String[] addresses = urlServer.substring(0, slash).split(",");
	      StringBuilder hosts = new StringBuilder();
	      StringBuilder ports = new StringBuilder();
	      for (String address : addresses) {
	        int portIdx = address.lastIndexOf(':');
	        if (portIdx != -1 && address.lastIndexOf(']') < portIdx) {
	          String portStr = address.substring(portIdx + 1);
	          try {
	            int port = Integer.parseInt(portStr);
	            if (port < 1 || port > 65535) {
	              return null;
	            }
	          } catch (NumberFormatException ignore) {
	            return null;
	          }
	          ports.append(portStr);
	          hosts.append(address.subSequence(0, portIdx));
	        } else {
	          ports.append(DEFAULT_PORT);
	          hosts.append(address);
	        }
	        ports.append(',');
	        hosts.append(',');
	      }
	      ports.setLength(ports.length() - 1);
	      hosts.setLength(hosts.length() - 1);
	      urlProps.setProperty("PORT", ports.toString());
	      urlProps.setProperty("HOST", hosts.toString());
	    } else {
	      /*
	       if there are no defaults set or any one of PORT, HOST, DBNAME not set
	       then set it to default
	      */
	      if (defaults == null || !defaults.containsKey("PORT")) {
	        urlProps.setProperty("PORT", DEFAULT_PORT);
	      }
	      if (defaults == null || !defaults.containsKey("HOST")) {
	        urlProps.setProperty("HOST", "localhost");
	      }
	      if (defaults == null || !defaults.containsKey("DBNAME")) {
	        urlProps.setProperty("DBNAME", URLCoder.decode(urlServer));
	      }
	    }
    }

    // parse the args part of the url
    String[] args = urlArgs.split("[;&]");
    for (String token : args) {
      if (token.isEmpty()) {
        continue;
      }
      int pos = token.indexOf('=');
      if (pos == -1) {
        urlProps.setProperty(token, "");
      } else {
        urlProps.setProperty(token.substring(0, pos), URLCoder.decode(token.substring(pos + 1)));
      }
    }

    return urlProps;
  }

  /**
   * 
   * @param props the connection properties.
   * @return the address portion of the URL
   */
  public static HostSpec[] hostSpecs(Properties props) {
  	HostSpec[] hostSpecs = null;
  	
  	String hostProp = props.getProperty("HOST");
  	String portProp = props.getProperty("PORT");
  	
  	if (hostProp != null && portProp != null) {
	    String[] hosts = hostProp.split(",");
	    String[] ports = portProp.split(",");
	    hostSpecs = new HostSpec[hosts.length];
	    for (int i = 0; i < hostSpecs.length; ++i) {
	      hostSpecs[i] = new HostSpec(hosts[i], Integer.parseInt(ports[i]));
	    }
  	}
    return hostSpecs;
  }

  /**
   * @return the username of the URL
   */
  private static String user(Properties props) {
	String user = props.getProperty("user");
	if(user == null)
	  user = props.getProperty("UID", "");
    return user;
  }

  /**
   * @return the database name of the URL
   */
  private static String database(Properties props) {
    return props.getProperty("DBNAME", "");
  }

  /**
   * @return the timeout from the URL, in milliseconds
   */
  private static long timeout(Properties props) {
    String timeout = RedshiftProperty.LOGIN_TIMEOUT.get(props);
    if (timeout != null) {
      try {
        return (long) (Float.parseFloat(timeout) * 1000);
      } catch (NumberFormatException e) {
      	// Ignore the error.
      }
    }
    return (long) DriverManager.getLoginTimeout() * 1000;
  }

  /**
   * This method was added in v6.5, and simply throws an SQLException for an unimplemented method. I
   * decided to do it this way while implementing the JDBC2 extensions to JDBC, as it should help
   * keep the overall driver size down. It now requires the call Class and the function name to help
   * when the driver is used with closed software that don't report the stack strace
   *
   * @param callClass the call Class
   * @param functionName the name of the unimplemented function with the type of its arguments
   * @return PSQLException with a localized message giving the complete description of the
   *         unimplemeted function
   */
  public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass,
      String functionName) {
    return new SQLFeatureNotSupportedException(
        GT.tr("Method {0} is not yet implemented.", callClass.getName() + "." + functionName),
        RedshiftState.NOT_IMPLEMENTED.getState());
  }

  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // java.util.logging.logger is not used in Redshift JDBC
    throw new SQLFeatureNotSupportedException ("java.util.logging is not used");
  }
  //JCP! endif

  public static SharedTimer getSharedTimer() {
    return sharedTimer;
  }

  /**
   * Register the driver against {@link DriverManager}. This is done automatically when the class is
   * loaded. Dropping the driver from DriverManager's list is possible using {@link #deregister()}
   * method.
   *
   * @throws IllegalStateException if the driver is already registered
   * @throws SQLException if registering the driver fails
   */
  public static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          "Driver is already registered. It can only be registered once.");
    }
    Driver registeredDriver = new Driver();
    DriverManager.registerDriver(registeredDriver);
    Driver.registeredDriver = registeredDriver;
  }

  /**
   * According to JDBC specification, this driver is registered against {@link DriverManager} when
   * the class is loaded. To avoid leaks, this method allow unregistering the driver so that the
   * class can be gc'ed if necessary.
   *
   * @throws IllegalStateException if the driver is not registered
   * @throws SQLException if deregistering the driver fails
   */
  public static void deregister() throws SQLException {
    if (!isRegistered()) {
      throw new IllegalStateException(
          "Driver is not registered (or it has not been registered using Driver.register() method)");
    }
    DriverManager.deregisterDriver(registeredDriver);
    registeredDriver = null;
  }

  /**
   * @return {@code true} if the driver is registered against {@link DriverManager}
   */
  public static boolean isRegistered() {
    return registeredDriver != null;
  }

  /**
   * Get JDBC INI file, if any exist.
   * 
   * Default file name is rsjdbc.ini.
   *  
   * The file location is in the following search order in the driver:
   * 
	 * 1. IniFile as connection parameter either in URL or in connection property. IniFile must be full path including file name
   * 2. The environment variable such as AMAZON_REDSHIFT_JDBC_INI_FILE with full path with any name of the file user wants
   * 3. The directory from where the driver jar get loaded 
   * 4. The user home directory
   * 5. The temp directory of the system
   * 
   * @param props
   * @return file name if exist otherwise null.
   * @throws RedshiftException 
   */
  private String getJdbcIniFile(Properties props) throws RedshiftException  {
  	// 1. Get file name from URL/property
  	String fileName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.INI_FILE.getName(), props);;
  	
  	if (!isFileExist(fileName, true)) {
  		// 2. Get file name from environment variable
  		fileName = System.getenv("AMAZON_REDSHIFT_JDBC_INI_FILE");
  		if (!isFileExist(fileName, true)) {
  			// 3. Get Driver Jar file location
  			String filePath = Driver.class.getProtectionDomain().getCodeSource().getLocation().getPath();					
  			
  			filePath = filePath.substring(0,filePath.lastIndexOf("/") );
  			fileName = getIniFullFileName(filePath);
  			
  			if(!isFileExist(fileName, false)) {
  				// 4. Get USER directory
  				filePath = System.getProperty("user.home");
    			fileName = getIniFullFileName(filePath);
    			if(!isFileExist(fileName, false)) {
    				// 5. Get temp directory
    				filePath = System.getProperty("java.io.tmpdir");
      			fileName = getIniFullFileName(filePath);
    				if(!isFileExist(fileName, false))
    					fileName = null;
    			}
  			}
  		}
  	}
  	
  	
  	return fileName;
  }
  
  private String getIniFullFileName(String filePath) {
  	String fileName = null;
  	
		if(filePath != null && filePath.length() > 0) {
			fileName = filePath 
									+ File.separator 
									+ DEFAULT_INI_FILE;
		}
  	
		return fileName;
  }
  
  /**
   * 
   * @param fileName full file name including path
   * @param fileMustExist
   * @return
   * @throws RedshiftException
   */
  private boolean isFileExist(String fileName, boolean fileMustExist) throws RedshiftException {
  	boolean fileExist = false;
  	
  	if (fileName != null && fileName.length() > 0) {
  		File file = new File(fileName);
  		if(!file.exists()) {
  			if (fileMustExist) {
	        throw new RedshiftException(
	            GT.tr("JDBC INI file doesn't exist: ")
	                + fileName,
	            RedshiftState.UNEXPECTED_ERROR);
  			}
  		}
  		else
  			fileExist = true;
  	}
  	
  	return fileExist;
  }
  
  /**
   * Read JDBC INI file and load properties.
   * 
   * The driver loads connection properties as follows:
	 * 1. Load default properties values as in the code
   * 2. Load [DRIVER] section properties from the INI file, if exist
   * 3. Load custom section properties from the INI file, if *IniSection* provided in the connection
   * 4. Load properties from connection property object given in the getConnection() call.
   * 5. Load properties from URL
   * 
   * @param fileName
   * @param props
   * @return
   * @throws RedshiftException 
   */
  private Properties readJdbcIniFile(String fileName, Properties props) throws RedshiftException {
  	String connectionSectionName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.INI_SECTION.getName(), props);;
  	String driverSectionName = DEFAULT_DRIVER_SECTION;
  	try {
			IniFile iniFile = new IniFile(fileName);
			Map<String, String> kv;
			Properties driverSectionProps = null;
			Properties connectionSectionProps = null;
			
			// Load properties from DRIVER section
			kv = iniFile.getAllKeyVals(driverSectionName);
			if (kv != null) {
				driverSectionProps = new Properties();
				driverSectionProps.putAll(kv);
			}
			
			// Load properties from IniSection provided by user
			if (connectionSectionName != null) {
				kv = iniFile.getAllKeyVals(connectionSectionName);
				if(kv != null) {
					connectionSectionProps = new Properties();
					connectionSectionProps.putAll(kv);
				}
				else {
	        throw new RedshiftException(
	            GT.tr("User specified section " + connectionSectionName
	            			 + " not found in the JDBC INI file "
	            			 + fileName),
	            RedshiftState.UNEXPECTED_ERROR);
				}
			}
				
			if (driverSectionProps != null
					|| connectionSectionProps != null) {
				
				// Get default properties from original props
				Properties iniProps = new Properties(props);
				
				// Add driver section props
				if (driverSectionProps != null) {
					iniProps.putAll(driverSectionProps);
				}

				// Add IniSection props
				if (connectionSectionProps != null) {
					iniProps.putAll(connectionSectionProps);
				}
				
				// URL and user connection props override INI pros
				iniProps.putAll(props);
				
				props = iniProps;
			}
		} 
  	catch (IOException e) {
	      throw new RedshiftException(
	          	GT.tr("Error loading JDBC INI file: ")
	              + fileName,
	              RedshiftState.UNEXPECTED_ERROR,
	              e);
		}
  	
  	return props;
  }
}
