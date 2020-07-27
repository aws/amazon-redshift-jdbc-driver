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

import org.ietf.jgss.GSSCredential;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

public class MakeGSS {

  public static void authenticate(RedshiftStream pgStream, String host, String user, String password,
      String jaasApplicationName, String kerberosServerName, boolean useSpnego, boolean jaasLogin,
      boolean logServerErrorDetail, RedshiftLogger logger)
          throws IOException, SQLException {
  	if(RedshiftLogger.isEnable())
  		logger.log(LogLevel.DEBUG, " <=BE AuthenticationReqGSS");

    if (jaasApplicationName == null) {
      jaasApplicationName = "rsjdbc";
    }
    if (kerberosServerName == null) {
      kerberosServerName = "postgres";
    }

    Exception result;
    try {
      boolean performAuthentication = jaasLogin;
      GSSCredential gssCredential = null;
      Subject sub = Subject.getSubject(AccessController.getContext());
      if (sub != null) {
        Set<GSSCredential> gssCreds = sub.getPrivateCredentials(GSSCredential.class);
        if (gssCreds != null && !gssCreds.isEmpty()) {
          gssCredential = gssCreds.iterator().next();
          performAuthentication = false;
        }
      }
      if (performAuthentication) {
        LoginContext lc =
            new LoginContext(jaasApplicationName, new GSSCallbackHandler(user, password));
        lc.login();
        sub = lc.getSubject();
      }
      PrivilegedAction<Exception> action = new GssAction(pgStream, gssCredential, host, user,
          kerberosServerName, useSpnego, logServerErrorDetail, logger);

      result = Subject.doAs(sub, action);
    } catch (Exception e) {
      throw new RedshiftException(GT.tr("GSS Authentication failed"), RedshiftState.CONNECTION_FAILURE, e);
    }

    if (result instanceof IOException) {
      throw (IOException) result;
    } else if (result instanceof SQLException) {
      throw (SQLException) result;
    } else if (result != null) {
      throw new RedshiftException(GT.tr("GSS Authentication failed"), RedshiftState.CONNECTION_FAILURE,
          result);
    }

  }

}
