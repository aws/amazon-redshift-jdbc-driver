/*
 * Copyright (c) 2008, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.gss;

import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.amazon.redshift.util.ServerErrorMessage;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.security.PrivilegedAction;

class GssAction implements PrivilegedAction<Exception> {

  private RedshiftLogger logger;
  private final RedshiftStream pgStream;
  private final String host;
  private final String user;
  private final String kerberosServerName;
  private final boolean useSpnego;
  private final GSSCredential clientCredentials;
  private final boolean logServerErrorDetail;

  GssAction(RedshiftStream pgStream, GSSCredential clientCredentials, String host, String user,
      String kerberosServerName, boolean useSpnego, boolean logServerErrorDetail,
      RedshiftLogger logger) {
  	this.logger = logger;
    this.pgStream = pgStream;
    this.clientCredentials = clientCredentials;
    this.host = host;
    this.user = user;
    this.kerberosServerName = kerberosServerName;
    this.useSpnego = useSpnego;
    this.logServerErrorDetail = logServerErrorDetail;
  }

  private static boolean hasSpnegoSupport(GSSManager manager) throws GSSException {
    org.ietf.jgss.Oid spnego = new org.ietf.jgss.Oid("1.3.6.1.5.5.2");
    org.ietf.jgss.Oid[] mechs = manager.getMechs();

    for (Oid mech : mechs) {
      if (mech.equals(spnego)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Exception run() {
    try {
      GSSManager manager = GSSManager.getInstance();
      GSSCredential clientCreds = null;
      Oid[] desiredMechs = new Oid[1];
      if (clientCredentials == null) {
        if (useSpnego && hasSpnegoSupport(manager)) {
          desiredMechs[0] = new Oid("1.3.6.1.5.5.2");
        } else {
          desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
        }
        GSSName clientName = manager.createName(user, GSSName.NT_USER_NAME);
        clientCreds = manager.createCredential(clientName, 8 * 3600, desiredMechs,
            GSSCredential.INITIATE_ONLY);
      } else {
        desiredMechs[0] = new Oid("1.2.840.113554.1.2.2");
        clientCreds = clientCredentials;
      }

      GSSName serverName =
          manager.createName(kerberosServerName + "@" + host, GSSName.NT_HOSTBASED_SERVICE);

      GSSContext secContext = manager.createContext(serverName, desiredMechs[0], clientCreds,
          GSSContext.DEFAULT_LIFETIME);
      secContext.requestMutualAuth(true);

      byte[] inToken = new byte[0];
      byte[] outToken = null;

      boolean established = false;
      while (!established) {
        outToken = secContext.initSecContext(inToken, 0, inToken.length);

        if (outToken != null) {
        	if(RedshiftLogger.isEnable())
        		logger.log(LogLevel.DEBUG, " FE=> Password(GSS Authentication Token)");

          pgStream.sendChar('p');
          pgStream.sendInteger4(4 + outToken.length);
          pgStream.send(outToken);
          pgStream.flush();
        }

        if (!secContext.isEstablished()) {
          int response = pgStream.receiveChar();
          // Error
          switch (response) {
            case 'E':
              int elen = pgStream.receiveInteger4();
              ServerErrorMessage errorMsg
                  = new ServerErrorMessage(pgStream.receiveErrorString(elen - 4));

            	if(RedshiftLogger.isEnable())
            		logger.log(LogLevel.DEBUG, " <=BE ErrorMessage({0})", errorMsg);

              return new RedshiftException(errorMsg, logServerErrorDetail);
            case 'R':
            	
            	if(RedshiftLogger.isEnable())
            		logger.log(LogLevel.DEBUG, " <=BE AuthenticationGSSContinue");
              int len = pgStream.receiveInteger4();
              int type = pgStream.receiveInteger4();
              // should check type = 8
              inToken = pgStream.receive(len - 8);
              break;
            default:
              // Unknown/unexpected message type.
              return new RedshiftException(GT.tr("Protocol error.  Session setup failed."),
                  RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
          }
        } else {
          established = true;
        }
      }

    } catch (IOException e) {
      return e;
    } catch (GSSException gsse) {
      return new RedshiftException(GT.tr("GSS Authentication failed"), RedshiftState.CONNECTION_FAILURE,
          gsse);
    }

    return null;
  }
}
