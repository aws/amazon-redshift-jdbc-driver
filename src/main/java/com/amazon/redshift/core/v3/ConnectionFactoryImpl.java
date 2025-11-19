/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core.v3;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.core.ConnectionFactory;
import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.core.QueryExecutor;
import com.amazon.redshift.core.ServerVersion;
import com.amazon.redshift.core.SetupQueryRunner;
import com.amazon.redshift.core.SocketFactoryFactory;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.core.Version;
import com.amazon.redshift.hostchooser.CandidateHost;
import com.amazon.redshift.hostchooser.GlobalHostStatusTracker;
import com.amazon.redshift.hostchooser.HostChooser;
import com.amazon.redshift.hostchooser.HostChooserFactory;
import com.amazon.redshift.hostchooser.HostRequirement;
import com.amazon.redshift.hostchooser.HostStatus;
import com.amazon.redshift.jdbc.SslMode;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.sspi.ISSPIClient;
import com.amazon.redshift.util.DriverInfo;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.HostSpec;
import com.amazon.redshift.util.MD5Digest;
import com.amazon.redshift.util.ExtensibleDigest;
import com.amazon.redshift.util.RedshiftConstants;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.ServerErrorMessage;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import javax.net.SocketFactory;

/**
 * ConnectionFactory implementation for version 3 (7.4+) connections.
 *
 * @author Oliver Jowett (oliver@opencloud.com), based on the previous implementation
 */
public class ConnectionFactoryImpl extends ConnectionFactory {

  private RedshiftLogger logger;
  private static final int AUTH_REQ_OK = 0;
  private static final int AUTH_REQ_KRB4 = 1;
  private static final int AUTH_REQ_KRB5 = 2;
  private static final int AUTH_REQ_PASSWORD = 3;
  private static final int AUTH_REQ_CRYPT = 4;
  private static final int AUTH_REQ_MD5 = 5;
  private static final int AUTH_REQ_SCM = 6;
  private static final int AUTH_REQ_GSS = 7;
  private static final int AUTH_REQ_GSS_CONTINUE = 8;
  private static final int AUTH_REQ_SSPI = 9;
  private static final int AUTH_REQ_SASL = 10;
  private static final int AUTH_REQ_SASL_CONTINUE = 11;
  private static final int AUTH_REQ_SASL_FINAL = 12;
  private static final int AUTH_REQ_DIGEST = 13;
  private static final int AUTH_REQ_IDP = 14; /* Redshift Native IDP Integration */
  
  private static final int AUTH_DIGEST_SHA256 = 0;
  private static final int AUTH_DIGEST_SCRYPT = 1;
  private static final int AUTH_DIGEST_ARGON2 = 2;
  
  // Server protocol versions
  public static int BASE_SERVER_PROTOCOL_VERSION = 0;
  public static int EXTENDED_RESULT_METADATA_SERVER_PROTOCOL_VERSION = 1;
  public static int BINARY_PROTOCOL_VERSION = 2;
  public static int EXTENDED2_RESULT_METADATA_SERVER_PROTOCOL_VERSION = 3; // Case sensitivity via COLLATION_INFORMATION
  public static int DEFAULT_SERVER_PROTOCOL_VERSION = EXTENDED2_RESULT_METADATA_SERVER_PROTOCOL_VERSION;

  private static final String IDP_TYPE_AWS_IDC = "AwsIdc";
  private static final String IDP_TYPE_OKTA = "Okta";
  private static final String IDP_TYPE_AZUREAD = "AzureAD";

  private static final String TOKEN_TYPE_ACCESS_TOKEN = "ACCESS_TOKEN";
  
  private ISSPIClient createSSPI(RedshiftStream pgStream,
      String spnServiceClass,
      boolean enableNegotiate) {
    try {
      @SuppressWarnings("unchecked")
      Class<ISSPIClient> c = (Class<ISSPIClient>) Class.forName("com.amazon.redshift.sspi.SSPIClient");
      return c.getDeclaredConstructor(RedshiftStream.class, String.class, boolean.class)
          .newInstance(pgStream, spnServiceClass, enableNegotiate);
    } catch (Exception e) {
      // This catched quite a lot exceptions, but until Java 7 there is no ReflectiveOperationException
      throw new IllegalStateException("Unable to load com.amazon.redshift.sspi.SSPIClient."
          + " Please check that SSPIClient is included in your pgjdbc distribution.", e);
    }
  }

  private RedshiftStream tryConnect(String user, String database,
      Properties info, SocketFactory socketFactory, HostSpec hostSpec,
      SslMode sslMode)
      throws SQLException, IOException {
    int connectTimeout = RedshiftProperty.CONNECT_TIMEOUT.getInt(info) * 1000;

    RedshiftStream newStream = null;

    try
    {
        newStream = constructNewStream(socketFactory, hostSpec, connectTimeout, logger, true, info);

        // Construct and send an ssl startup packet if requested.
        newStream = enableSSL(newStream, sslMode, info, connectTimeout);
        List<String[]> paramList = getParametersForStartup(user, database, info, true);
        sendStartupPacket(newStream, paramList);
        newStream.changeStream(false, info);
        // Set the socket timeout if the "socketTimeout" property has been set.
        int socketTimeout = RedshiftProperty.SOCKET_TIMEOUT.getInt(info);
        if (socketTimeout > 0) {
          newStream.setNetworkTimeout(socketTimeout * 1000);
        }
        // Do authentication (until AuthenticationOk).
        doAuthentication(newStream, hostSpec.getHost(), user, info);
    }
    catch(Exception ex) {
      closeStream(newStream);
      throw ex;
    }

    return newStream;
  }

  public RedshiftStream constructNewStream(SocketFactory socketFactory, HostSpec hostSpec, int connectTimeout, RedshiftLogger logger, Boolean disableCompressionForSSL, Properties info) throws SQLException, IOException
  {
    RedshiftStream newStream = new RedshiftStream(socketFactory, hostSpec, connectTimeout, logger, disableCompressionForSSL, info);

    // Set the socket timeout if the "socketTimeout" property has been set.
    int socketTimeout = RedshiftProperty.SOCKET_TIMEOUT.getInt(info);
    if (socketTimeout > 0) {
      newStream.getSocket().setSoTimeout(socketTimeout * 1000);
    }

    String maxResultBuffer = RedshiftProperty.MAX_RESULT_BUFFER.get(info);
    newStream.setMaxResultBuffer(maxResultBuffer);

    // Enable TCP keep-alive probe if required.
    boolean requireTCPKeepAlive = RedshiftProperty.TCP_KEEP_ALIVE.getBoolean(info);
    newStream.getSocket().setKeepAlive(requireTCPKeepAlive);

    // Try to set SO_SNDBUF and SO_RECVBUF socket options, if requested.
    // If receiveBufferSize and send_buffer_size are set to a value greater
    // than 0, adjust. -1 means use the system default, 0 is ignored since not
    // supported.

    // Set SO_RECVBUF read buffer size
    int receiveBufferSize = RedshiftProperty.RECEIVE_BUFFER_SIZE.getInt(info);
    if (receiveBufferSize > -1) {
      // value of 0 not a valid buffer size value
      if (receiveBufferSize > 0) {
        newStream.getSocket().setReceiveBufferSize(receiveBufferSize);
      } else {
        if(RedshiftLogger.isEnable())
          logger.log(LogLevel.INFO, "Ignore invalid value for receiveBufferSize: {0}", receiveBufferSize);
      }
    }

    // Set SO_SNDBUF write buffer size
    int sendBufferSize = RedshiftProperty.SEND_BUFFER_SIZE.getInt(info);
    if (sendBufferSize > -1) {
      if (sendBufferSize > 0) {
        newStream.getSocket().setSendBufferSize(sendBufferSize);
      } else {
        if(RedshiftLogger.isEnable())
          logger.log(LogLevel.INFO, "Ignore invalid value for sendBufferSize: {0}", sendBufferSize);
      }
    }

    if(RedshiftLogger.isEnable()) {
      logger.log(LogLevel.DEBUG, "Receive Buffer Size is {0}", newStream.getSocket().getReceiveBufferSize());
      logger.log(LogLevel.DEBUG, "Send Buffer Size is {0}", newStream.getSocket().getSendBufferSize());
    }

    return newStream;
  }

  @Override
  public QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user, String database,
      Properties info, RedshiftLogger logger) throws SQLException {
    this.logger = logger;
  	SslMode sslMode = SslMode.of(info);

    HostRequirement targetServerType;
    String targetServerTypeStr = RedshiftProperty.TARGET_SERVER_TYPE.get(info);
    try {
      targetServerType = HostRequirement.getTargetServerType(targetServerTypeStr);
    } catch (IllegalArgumentException ex) {
      throw new RedshiftException(
          GT.tr("Invalid targetServerType value: {0}", targetServerTypeStr),
          RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
    }

    SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(info);

    HostChooser hostChooser =
        HostChooserFactory.createHostChooser(hostSpecs, targetServerType, info);
    Iterator<CandidateHost> hostIter = hostChooser.iterator();
    Map<HostSpec, HostStatus> knownStates = new HashMap<HostSpec, HostStatus>();
    while (hostIter.hasNext()) {
      CandidateHost candidateHost = hostIter.next();
      HostSpec hostSpec = candidateHost.hostSpec;
    	if(RedshiftLogger.isEnable())
    		logger.log(LogLevel.DEBUG, "Trying to establish a protocol version 3 connection to {0}", hostSpec);

      // Note: per-connect-attempt status map is used here instead of GlobalHostStatusTracker
      // for the case when "no good hosts" match (e.g. all the hosts are known as "connectfail")
      // In that case, the system tries to connect to each host in order, thus it should not look into
      // GlobalHostStatusTracker
      HostStatus knownStatus = knownStates.get(hostSpec);
      if (knownStatus != null && !candidateHost.targetServerType.allowConnectingTo(knownStatus)) {
      	if(RedshiftLogger.isEnable()) {
          logger.log(LogLevel.DEBUG, "Known status of host {0} is {1}, and required status was {2}. Will try next host",
                     new Object[]{hostSpec, knownStatus, candidateHost.targetServerType});
        }
        continue;
      }

      //
      // Establish a connection.
      //

      RedshiftStream newStream = null;
      try {
        try {
          newStream = tryConnect(user, database, info, socketFactory, hostSpec, sslMode);
        } catch (SQLException e) {
          if (sslMode == SslMode.PREFER
              && RedshiftState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(e.getSQLState())) {
            // Try non-SSL connection to cover case like "non-ssl only db"
            // Note: PREFER allows loss of encryption, so no significant harm is made
            Throwable ex = null;
            try {
              newStream =
                  tryConnect(user, database, info, socketFactory, hostSpec, SslMode.DISABLE);
            	if(RedshiftLogger.isEnable())
            		logger.log(LogLevel.DEBUG, "Downgraded to non-encrypted connection for host {0}",
                  hostSpec);
            } catch (SQLException ee) {
              ex = ee;
            } catch (IOException ee) {
              ex = ee; // Can't use multi-catch in Java 6 :(
            }
            if (ex != null) {
            	if(RedshiftLogger.isEnable())
            		logger.log(LogLevel.DEBUG, ex, "sslMode==PREFER, however non-SSL connection failed as well");
              // non-SSL failed as well, so re-throw original exception
              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
              // Add non-SSL exception as suppressed
              e.addSuppressed(ex);
              //JCP! endif
              throw e;
            }
          } else if (sslMode == SslMode.ALLOW
              && RedshiftState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(e.getSQLState())) {
            // Try using SSL
            Throwable ex = null;
            try {
              newStream =
                  tryConnect(user, database, info, socketFactory, hostSpec, SslMode.REQUIRE);
              if(RedshiftLogger.isEnable())
              	logger.log(LogLevel.DEBUG, "Upgraded to encrypted connection for host {0}",
              							hostSpec);
            } catch (SQLException ee) {
              ex = ee;
            } catch (IOException ee) {
              ex = ee; // Can't use multi-catch in Java 6 :(
            }
            if (ex != null) {
            	if(RedshiftLogger.isEnable())
            		logger.log(LogLevel.DEBUG, ex, "sslMode==ALLOW, however SSL connection failed as well");
              // non-SSL failed as well, so re-throw original exception
              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
              // Add SSL exception as suppressed
              e.addSuppressed(ex);
              //JCP! endif
              throw e;
            }

          } else {
            throw e;
          }
        }

        int cancelSignalTimeout = RedshiftProperty.CANCEL_SIGNAL_TIMEOUT.getInt(info) * 1000;

        // Do final startup.
        QueryExecutor queryExecutor = new QueryExecutorImpl(newStream, user, database,
            cancelSignalTimeout, info, logger);

        // Check Primary or Secondary
        HostStatus hostStatus = HostStatus.ConnectOK;
        if (candidateHost.targetServerType != HostRequirement.any) {
          hostStatus = isPrimary(queryExecutor) ? HostStatus.Primary : HostStatus.Secondary;
        }
        GlobalHostStatusTracker.reportHostStatus(hostSpec, hostStatus);
        knownStates.put(hostSpec, hostStatus);
        if (!candidateHost.targetServerType.allowConnectingTo(hostStatus)) {
          queryExecutor.close();
          continue;
        }

        runInitialQueries(queryExecutor, info);

        // And we're done.
        return queryExecutor;
      } catch (ConnectException cex) {
        // Added by Peter Mount <peter@retep.org.uk>
        // ConnectException is thrown when the connection cannot be made.
        // we trap this an return a more meaningful message for the end user
        GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail);
        knownStates.put(hostSpec, HostStatus.ConnectFail);
        if (hostIter.hasNext()) {
        	if(RedshiftLogger.isEnable())
        		logger.log(LogLevel.DEBUG, cex, "ConnectException occurred while connecting to {0}", hostSpec);
          // still more addresses to try
          continue;
        }
        throw new RedshiftException(GT.tr(
            "Connection to {0} refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
            hostSpec), RedshiftState.CONNECTION_UNABLE_TO_CONNECT, cex);
      } catch (IOException ioe) {
        closeStream(newStream);
        GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail);
        knownStates.put(hostSpec, HostStatus.ConnectFail);
        if (hostIter.hasNext()) {
        	if(RedshiftLogger.isEnable())
        		logger.log(LogLevel.DEBUG, ioe, "IOException occurred while connecting to {0}", hostSpec);
          // still more addresses to try
          continue;
        }
        throw new RedshiftException(GT.tr("The connection attempt failed."),
            RedshiftState.CONNECTION_UNABLE_TO_CONNECT, ioe);
      } catch (SQLException se) {
        closeStream(newStream);
        GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail);
        knownStates.put(hostSpec, HostStatus.ConnectFail);
        if (hostIter.hasNext()) {
        	if(RedshiftLogger.isEnable())
        		logger.log(LogLevel.DEBUG, se, "SQLException occurred while connecting to {0}", hostSpec);
          // still more addresses to try
          continue;
        }
        throw se;
      }
    }
    throw new RedshiftException(GT
        .tr("Could not find a server with specified targetServerType: {0}", targetServerType),
        RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
  }

  private List<String[]> getParametersForStartup(String user, String database, Properties info, boolean driverOsVersionParams) {
    List<String[]> paramList = new ArrayList<String[]>();
    boolean redshiftNativeAuth = false;
    String idpType = "";
    String tokenType = "";
    String identityNamepsace = "";
    String idcClientDisplayName = "";

    String pluginName = RedshiftProperty.CREDENTIALS_PROVIDER.get(info);
    if(RedshiftLogger.isEnable())
      logger.log(LogLevel.INFO, "using plugin: " + pluginName);
    if(pluginName != null)
    {
      if(pluginName.equalsIgnoreCase(RedshiftConstants.BASIC_JWT_PLUGIN)
              || pluginName.equalsIgnoreCase(RedshiftConstants.NATIVE_IDP_AZUREAD_BROWSER_PLUGIN))
      {
        idpType = IDP_TYPE_AZUREAD;
        redshiftNativeAuth = true;
      }
      else if(pluginName.equalsIgnoreCase(RedshiftConstants.NATIVE_IDP_OKTA_BROWSER_PLUGIN)||
              pluginName.equalsIgnoreCase(RedshiftConstants.NATIVE_IDP_OKTA_NON_BROWSER_PLUGIN))
      {
        idpType = IDP_TYPE_OKTA;
        redshiftNativeAuth = true;
      }
      else if(pluginName.equalsIgnoreCase(RedshiftConstants.IDP_TOKEN_PLUGIN))
      {
        idpType = IDP_TYPE_AWS_IDC;
        identityNamepsace = RedshiftProperty.IDC_IDENTITY_NAMESPACE.get(info);
        tokenType = RedshiftProperty.TOKEN_TYPE.get(info);
        redshiftNativeAuth = true;
      }
      else if(pluginName.equalsIgnoreCase(RedshiftConstants.IDC_PKCE_BROWSER_PLUGIN)) {
        idpType = IDP_TYPE_AWS_IDC;
        tokenType = TOKEN_TYPE_ACCESS_TOKEN;
        redshiftNativeAuth = true;
        idcClientDisplayName = RedshiftProperty.IDC_CLIENT_DISPLAY_NAME.get(info);
      }
    }
    
    if(!redshiftNativeAuth)
      paramList.add(new String[]{"user", user});
    else {
      if(user != null && user.length() > 0)
        paramList.add(new String[]{"user", user});
    }
    
    paramList.add(new String[]{"database", database});
    paramList.add(new String[]{"client_encoding", "UTF8"});
    paramList.add(new String[]{"DateStyle", "ISO"});

    if(RedshiftProperty.CONNECTION_TIMEZONE.get(info).equalsIgnoreCase("LOCAL"))
    {
      // Sets session level timezone to JVM timezone.
      paramList.add(new String[]{"TimeZone", createRedshiftTimeZone()});
    }

    if(!("off".equalsIgnoreCase(RedshiftProperty.COMPRESSION.get(info))))
    {
      paramList.add(new String[]{"_pq_.compression", info.getProperty("compression", RedshiftProperty.COMPRESSION.get(info))});
    }

    Version assumeVersion = ServerVersion.from(RedshiftProperty.ASSUME_MIN_SERVER_VERSION.get(info));

    if (assumeVersion.getVersionNum() >= ServerVersion.v9_0.getVersionNum()) {
      // User is explicitly telling us this is a 9.0+ server so set properties here:
      paramList.add(new String[]{"extra_float_digits", "3"});
    } else {
      // User has not explicitly told us that this is a 9.0+ server so stick to old default:
      paramList.add(new String[]{"extra_float_digits", "2"});
    }
    
    String appName = RedshiftProperty.APPLICATION_NAME.get(info);
    if(appName == null)
    {
      StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
      appName = "[" + Thread.currentThread().getName() + "]"
                        + stacktrace[stacktrace.length-1].toString();
    }
    
    if (appName != null) {
      paramList.add(new String[]{"application_name", appName});
    }
    
    if(driverOsVersionParams) {
	    String driver_version = DriverInfo.DRIVER_FULL_NAME;
	    paramList.add(new String[]{"driver_version",driver_version});
	
	    String os_version = "";
	    try {
	    	os_version = System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch");
	    } 
	    catch (Exception e) {
	      os_version = "Unknown";
	    }
	    paramList.add(new String[]{"os_version",os_version});
	    
	    if (pluginName != null && pluginName.length() != 0) {
		    paramList.add(new String[]{"plugin_name",pluginName});
	    }
	    
	    // Send protocol version as 2, so server can support Binary protocol (v2), send optimized extended RSMD (v1).
	    String clientProtocolVersion = info.getProperty("client_protocol_version", Integer.toString(DEFAULT_SERVER_PROTOCOL_VERSION)); // Undocumented property to lower the protocol version.
	    paramList.add(new String[]{"client_protocol_version",clientProtocolVersion}); 
    } // New parameters
    
    // Redshift Native Auth values
    if(redshiftNativeAuth) {
      if (!Utils.isNullOrEmpty(idpType)) {
        paramList.add(new String[]{"idp_type",idpType});
      }
      if(RedshiftLogger.isEnable())
        logger.logDebug("Using idp_type=" + idpType);
      
      String providerName = RedshiftProperty.PROVIDER_NAME.get(info);
      if (!Utils.isNullOrEmpty(providerName)) {
        paramList.add(new String[]{"provider_name",providerName});
      }
      if(RedshiftLogger.isEnable())
        logger.logDebug("Using provider_name=" + providerName);

      if (!Utils.isNullOrEmpty(tokenType)) {
        paramList.add(new String[]{"token_type",tokenType});
      }
      if(RedshiftLogger.isEnable())
        logger.logDebug("Using token_type=" + tokenType);

      if (!Utils.isNullOrEmpty(identityNamepsace)) {
        paramList.add(new String[]{"identity_namespace",identityNamepsace});
      }
      if(RedshiftLogger.isEnable())
        logger.logDebug("Using identity_namespace=" + identityNamepsace);
        
      if (!Utils.isNullOrEmpty(idcClientDisplayName)) {
        paramList.add(new String[]{"idc_client_display_name", idcClientDisplayName});
      }
      if(RedshiftLogger.isEnable())
        logger.logDebug("Using idc_client_display_name=" + idcClientDisplayName);
    }
    
    

    String currentSchema = RedshiftProperty.CURRENT_SCHEMA.get(info);
    if (currentSchema != null) {
      paramList.add(new String[]{"search_path", currentSchema});
    }

    String options = RedshiftProperty.OPTIONS.get(info);
    if (options != null) {
      paramList.add(new String[]{"options", options});
    }

    return paramList;
  }

  /**
   * Convert Java time zone to Redshift time zone. All others stay the same except that GMT+nn
   * changes to GMT-nn and vise versa.
   *
   * @return The current JVM time zone in Redshift format.
   */
  private static String createRedshiftTimeZone() {
    String tz = TimeZone.getDefault().getID();
    if (tz.length() <= 3 || !tz.startsWith("GMT")) {
      return tz;
    }
    char sign = tz.charAt(3);
    String start;
    switch (sign) {
      case '+':
        start = "GMT-";
        break;
      case '-':
        start = "GMT+";
        break;
      default:
        // unknown type
        return tz;
    }

    return start + tz.substring(4);
  }

  private RedshiftStream enableSSL(RedshiftStream pgStream, SslMode sslMode, Properties info,
      int connectTimeout)
      throws IOException, RedshiftException {
    if (sslMode == SslMode.DISABLE) {
      return pgStream;
    }
    if (sslMode == SslMode.ALLOW) {
      // Allow ==> start with plaintext, use encryption if required by server
      return pgStream;
    }

    if(RedshiftLogger.isEnable())
    	logger.log(LogLevel.DEBUG, " FE=> SSLRequest");

    // Send SSL request packet
    pgStream.sendInteger4(8);
    pgStream.sendInteger2(1234);
    pgStream.sendInteger2(5679);
    pgStream.flush();

    // Now get the response from the backend, one of N, E, S.
    int beresp = pgStream.receiveChar();
    switch (beresp) {
      case 'E':
        if(RedshiftLogger.isEnable())
        	logger.log(LogLevel.DEBUG, " <=BE SSLError");
        
        // An error occurred, so pass the error message to the
        // user.
        //
        int elen = pgStream.receiveInteger4();

        ServerErrorMessage errorMsg =
            new ServerErrorMessage(pgStream.receiveErrorString(elen - 4));
        
        if(RedshiftLogger.isEnable())
            logger.log(LogLevel.DEBUG, " <=BE ErrorMessage({0})", errorMsg);
        
        // Server doesn't even know about the SSL handshake protocol
        if (sslMode.requireEncryption()) {
          throw new RedshiftException(errorMsg, RedshiftProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(info));
          
//          throw new RedshiftException(GT.tr("The server does not support SSL."),
//              RedshiftState.CONNECTION_REJECTED);
        }

        // We have to reconnect to continue.
        pgStream.close();
        return new RedshiftStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), connectTimeout, logger, true, info);

      case 'N':
        if(RedshiftLogger.isEnable())
        	logger.log(LogLevel.DEBUG, " <=BE SSLRefused");

        // Server does not support ssl
        if (sslMode.requireEncryption()) {
          throw new RedshiftException(GT.tr("The server does not support SSL."),
              RedshiftState.CONNECTION_REJECTED);
        }

        return pgStream;

      case 'S':
        if(RedshiftLogger.isEnable())
        	logger.log(LogLevel.DEBUG, " <=BE SSLOk");

        // Server supports ssl
        com.amazon.redshift.ssl.MakeSSL.convert(pgStream, info);
        return pgStream;

      default:
        throw new RedshiftException(GT.tr("An error occurred while setting up the SSL connection."),
            RedshiftState.PROTOCOL_VIOLATION);
    }
  }

  private void sendStartupPacket(RedshiftStream pgStream, List<String[]> params)
      throws IOException {
    if(RedshiftLogger.isEnable()) {
      StringBuilder details = new StringBuilder();
      for (int i = 0; i < params.size(); ++i) {
        if (i != 0) {
          details.append(", ");
        }
        details.append(params.get(i)[0]);
        details.append("=");
        details.append(params.get(i)[1]);
      }
      logger.log(LogLevel.DEBUG, " FE=> StartupPacket({0})", details);
    }

    // Precalculate message length and encode params.
    int length = 4 + 4;
    byte[][] encodedParams = new byte[params.size() * 2][];
    for (int i = 0; i < params.size(); ++i) {
      encodedParams[i * 2] = params.get(i)[0].getBytes("UTF-8");
      encodedParams[i * 2 + 1] = params.get(i)[1].getBytes("UTF-8");
      length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
    }

    length += 1; // Terminating \0

    // Send the startup message.
    pgStream.sendInteger4(length);
    pgStream.sendInteger2(3); // protocol major
    pgStream.sendInteger2(0); // protocol minor
    for (byte[] encodedParam : encodedParams) {
      pgStream.send(encodedParam);
      pgStream.sendChar(0);
    }

    pgStream.sendChar(0);
    pgStream.flush();
  }

  private void doAuthentication(RedshiftStream pgStream, String host, String user, Properties info) throws IOException, SQLException {
    // Now get the response from the backend, either an error message
    // or an authentication request

    String password = RedshiftProperty.PASSWORD.get(info);
    if(null == password)
    {
      password = RedshiftProperty.PWD.get(info);
    }

    /* SSPI negotiation state, if used */
    ISSPIClient sspiClient = null;

    //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
    /* SCRAM authentication state, if used */
    //com.amazon.redshift.jre7.sasl.ScramAuthenticator scramAuthenticator =
    // null;
    //JCP! endif

    try {
      authloop: while (true) {
        int beresp = pgStream.receiveChar();

        switch (beresp) {
          case 'E':
            // An error occurred, so pass the error message to the
            // user.
            //
            // The most common one to be thrown here is:
            // "User authentication failed"
            //
            int elen = pgStream.receiveInteger4();

            ServerErrorMessage errorMsg =
                new ServerErrorMessage(pgStream.receiveErrorString(elen - 4));
            
            if(RedshiftLogger.isEnable())
            	logger.log(LogLevel.DEBUG, " <=BE ErrorMessage({0})", errorMsg);
            throw new RedshiftException(errorMsg, RedshiftProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(info));

          case 'R':
            // Authentication request.
            // Get the message length
            int msgLen = pgStream.receiveInteger4();

            // Get the type of request
            int areq = pgStream.receiveInteger4();

            // Process the request.
            switch (areq) {
              case AUTH_REQ_MD5: {
                byte[] md5Salt = pgStream.receive(4);
                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " <=BE AuthenticationReqMD5");
                }

                if (password == null) {
                  throw new RedshiftException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      RedshiftState.CONNECTION_REJECTED);
                }

                byte[] digest =
                    MD5Digest.encode(user.getBytes("UTF-8"), password.getBytes("UTF-8"), md5Salt);

                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " FE=> Password(md5digest)");
                }
                
                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + digest.length + 1);
                pgStream.send(digest);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_DIGEST: {
              	// Extensible user password hashing algorithm constant value 
                int algo = pgStream.receiveInteger4();
                String[] algoNames = { "SHA-256" };
              	
                int saltLen = pgStream.receiveInteger4();
                byte[] salt = pgStream.receive(saltLen);
                int serverNonceLen = pgStream.receiveInteger4();
                byte[] serverNonce = pgStream.receive(serverNonceLen);
                
                String dateTimeString = Long.toString(new Date().getTime());
                byte[] clientNonce = dateTimeString.getBytes();                
                
                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " <=BE AuthenticationReqDigest: Algo:" + algo);
                }
                

                if (password == null) {
                  throw new RedshiftException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      RedshiftState.CONNECTION_REJECTED);
                }

                if (algo > algoNames.length) {
                  throw new RedshiftException(
                      GT.tr(
                          "The server requested password-based authentication, but requested algorithm " + algo + " is not supported."),
                      RedshiftState.CONNECTION_REJECTED);
                }
                
                byte[] digest =
                    ExtensibleDigest.encode(clientNonce, 
                    								password.getBytes("UTF-8"), 
                    								salt,
                    								algoNames[algo],
                    								serverNonce);

                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " FE=> Password(extensible digest)");
                }
                
                pgStream.sendChar('d');
                pgStream.sendInteger4(4 + 4 + digest.length + 4 + clientNonce.length);
                pgStream.sendInteger4(digest.length);
                pgStream.send(digest);
                pgStream.sendInteger4(clientNonce.length);
                pgStream.send(clientNonce);
                pgStream.flush();

                break;
              }
              
              case AUTH_REQ_IDP: {
                String idpToken = RedshiftProperty.WEB_IDENTITY_TOKEN.get(info);

                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " <=BE AuthenticationReqIDP");
                }
                
                if (idpToken == null || idpToken.length() == 0) {
                  throw new RedshiftException(
                      GT.tr(
                          "The server requested IDP token-based authentication, but no token was provided."),
                      RedshiftState.CONNECTION_REJECTED);
                }

                if(RedshiftLogger.isEnable()) {
                  logger.log(LogLevel.DEBUG, " FE=> IDP(IDP Token)");
                }
                
                byte[] token = idpToken.getBytes("UTF-8");
                pgStream.sendChar('i');
                pgStream.sendInteger4(4 + token.length + 1);
                pgStream.send(token);
                pgStream.sendChar(0);
                pgStream.flush();
                
                break;
              }
              
              case AUTH_REQ_PASSWORD: {
                if(RedshiftLogger.isEnable()) {
	                logger.log(LogLevel.DEBUG, "<=BE AuthenticationReqPassword");
	                logger.log(LogLevel.DEBUG, " FE=> Password(password=<not shown>)");
                }

                if (password == null) {
                  throw new RedshiftException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      RedshiftState.CONNECTION_REJECTED);
                }

                byte[] encodedPassword = password.getBytes("UTF-8");

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + encodedPassword.length + 1);
                pgStream.send(encodedPassword);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_GSS:
              case AUTH_REQ_SSPI:
                /*
                 * Use GSSAPI if requested on all platforms, via JSSE.
                 *
                 * For SSPI auth requests, if we're on Windows attempt native SSPI authentication if
                 * available, and if not disabled by setting a kerberosServerName. On other
                 * platforms, attempt JSSE GSSAPI negotiation with the SSPI server.
                 *
                 * Note that this is slightly different to libpq, which uses SSPI for GSSAPI where
                 * supported. We prefer to use the existing Java JSSE Kerberos support rather than
                 * going to native (via JNA) calls where possible, so that JSSE system properties
                 * etc continue to work normally.
                 *
                 * Note that while SSPI is often Kerberos-based there's no guarantee it will be; it
                 * may be NTLM or anything else. If the client responds to an SSPI request via
                 * GSSAPI and the other end isn't using Kerberos for SSPI then authentication will
                 * fail.
                 */
                final String gsslib = RedshiftProperty.GSS_LIB.get(info);
                final boolean usespnego = RedshiftProperty.USE_SPNEGO.getBoolean(info);

                boolean useSSPI = false;

                /*
                 * Use SSPI if we're in auto mode on windows and have a request for SSPI auth, or if
                 * it's forced. Otherwise use gssapi. If the user has specified a Kerberos server
                 * name we'll always use JSSE GSSAPI.
                 */
                if (gsslib.equals("gssapi")) {
                  if(RedshiftLogger.isEnable())
                  	logger.log(LogLevel.DEBUG, "Using JSSE GSSAPI, param gsslib=gssapi");
                } else if (areq == AUTH_REQ_GSS && !gsslib.equals("sspi")) {
                  	if(RedshiftLogger.isEnable())
                  		logger.log(LogLevel.DEBUG,
                      "Using JSSE GSSAPI, gssapi requested by server and gsslib=sspi not forced");
                } else {
                  /* Determine if SSPI is supported by the client */
                  sspiClient = createSSPI(pgStream, RedshiftProperty.SSPI_SERVICE_CLASS.get(info),
                      /* Use negotiation for SSPI, or if explicitly requested for GSS */
                      areq == AUTH_REQ_SSPI || (areq == AUTH_REQ_GSS && usespnego));

                  useSSPI = sspiClient.isSSPISupported();
                  
                  if(RedshiftLogger.isEnable())
                  	logger.log(LogLevel.DEBUG, "SSPI support detected: {0}", useSSPI);

                  if (!useSSPI) {
                    /* No need to dispose() if no SSPI used */
                    sspiClient = null;

                    if (gsslib.equals("sspi")) {
                      throw new RedshiftException(
                          "SSPI forced with gsslib=sspi, but SSPI not available; set loglevel=2 for details",
                          RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
                    }
                  }

                  if(RedshiftLogger.isEnable()) {
                    logger.log(LogLevel.DEBUG, "Using SSPI: {0}, gsslib={1} and SSPI support detected", new Object[]{useSSPI, gsslib});
                  }
                }

                if (useSSPI) {
                  /* SSPI requested and detected as available */
                  sspiClient.startSSPI();
                } else {
                  /* Use JGSS's GSSAPI for this request */
                  com.amazon.redshift.gss.MakeGSS.authenticate(pgStream, host, user, password,
                      RedshiftProperty.JAAS_APPLICATION_NAME.get(info),
                      RedshiftProperty.KERBEROS_SERVER_NAME.get(info), usespnego,
                      RedshiftProperty.JAAS_LOGIN.getBoolean(info),
                      RedshiftProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(info),
                      logger);
                }
                break;

              case AUTH_REQ_GSS_CONTINUE:
                /*
                 * Only called for SSPI, as GSS is handled by an inner loop in MakeGSS.
                 */
                sspiClient.continueSSPI(msgLen - 8);
                break;

              case AUTH_REQ_SASL:
              	
                if(RedshiftLogger.isEnable())
                	logger.log(LogLevel.DEBUG, " <=BE AuthenticationSASL");

                //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
//                scramAuthenticator = new com.amazon.redshift.jre7.sasl.ScramAuthenticator(user, password, pgStream);
//                scramAuthenticator.processServerMechanismsAndInit();
//                scramAuthenticator.sendScramClientFirstMessage();
                // This works as follows:
                // 1. When tests is run from IDE, it is assumed SCRAM library is on the classpath
                // 2. In regular build for Java < 8 this `if` is deactivated and the code always throws
                if (false) {
                  //JCP! else
//JCP>                   throw new RedshiftException(GT.tr(
//JCP>                           "SCRAM authentication is not supported by this driver. You need JDK >= 8 and pgjdbc >= 42.2.0 (not \".jre\" versions)",
//JCP>                           areq), RedshiftState.CONNECTION_REJECTED);
                  //JCP! endif
                  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
                }
                break;
                //JCP! endif

              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.1"
//              case AUTH_REQ_SASL_CONTINUE:
//                scramAuthenticator.processServerFirstMessage(msgLen - 4 - 4);
//                break;
//
//              case AUTH_REQ_SASL_FINAL:
//                scramAuthenticator.verifyServerSignature(msgLen - 4 - 4);
//                break;
              //JCP! endif

              case AUTH_REQ_OK:
                /* Cleanup after successful authentication */
                if(RedshiftLogger.isEnable())
                	logger.log(LogLevel.DEBUG, " <=BE AuthenticationOk");
                break authloop; // We're done.

              default:
                if(RedshiftLogger.isEnable())
                	logger.log(LogLevel.DEBUG, " <=BE AuthenticationReq (unsupported type {0})", areq);
                
                throw new RedshiftException(GT.tr(
                    "The authentication type {0} is not supported. Check that you have configured the pg_hba.conf file to include the client''s IP address or subnet, and that it is using an authentication scheme supported by the driver.",
                    areq), RedshiftState.CONNECTION_REJECTED);
            }

            break;

          default:
            throw new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
                RedshiftState.PROTOCOL_VIOLATION);
        }
      }
    } finally {
      /* Cleanup after successful or failed authentication attempts */
      if (sspiClient != null) {
        try {
          sspiClient.dispose();
        } catch (RuntimeException ex) {
          if(RedshiftLogger.isEnable())
          	logger.log(LogLevel.DEBUG, ex, "Unexpected error during SSPI context disposal");
        }

      }
    }

  }

  private void runInitialQueries(QueryExecutor queryExecutor, Properties info)
      throws SQLException {
    String assumeMinServerVersion = RedshiftProperty.ASSUME_MIN_SERVER_VERSION.get(info);
    if (Utils.parseServerVersionStr(assumeMinServerVersion) >= ServerVersion.v9_0.getVersionNum()) {
      // We already sent the parameter values in the StartupMessage so skip this
      return;
    }

    final int dbVersion = queryExecutor.getServerVersionNum();

    if (dbVersion >= ServerVersion.v9_0.getVersionNum()) {
      SetupQueryRunner.run(queryExecutor, "SET extra_float_digits = 3", false);
    }

    String appName = RedshiftProperty.APPLICATION_NAME.get(info);
    if (appName != null && appName.length() != 0) { //  && dbVersion >= ServerVersion.v9_0.getVersionNum()
      StringBuilder sql = new StringBuilder();
      sql.append("SET application_name = '");
      Utils.escapeLiteral(sql, appName, queryExecutor.getStandardConformingStrings());
      sql.append("'");
      SetupQueryRunner.run(queryExecutor, sql.toString(), false);
    }

    String queryGroup = RedshiftProperty.QUERY_GROUP.get(info);
    if (queryGroup != null && queryGroup.length() != 0) { 
      StringBuilder sql = new StringBuilder();
      sql.append("SET query_group TO '");
      Utils.escapeLiteral(sql, queryGroup, queryExecutor.getStandardConformingStrings());
      sql.append("'");
      SetupQueryRunner.run(queryExecutor, sql.toString(), false);
    }
  }

  private boolean isPrimary(QueryExecutor queryExecutor) throws SQLException, IOException {
    Tuple results = SetupQueryRunner.run(queryExecutor, "show transaction_read_only", true);
    String value = queryExecutor.getEncoding().decode(results.get(0));
    return value.equalsIgnoreCase("off");
  }
}
