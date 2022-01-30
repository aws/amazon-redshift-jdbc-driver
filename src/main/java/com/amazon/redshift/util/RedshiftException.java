/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.sql.SQLException;

import com.amazon.redshift.logger.RedshiftLogger;

public class RedshiftException extends SQLException {

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ServerErrorMessage serverError;

  public RedshiftException(String msg, RedshiftState state, Throwable cause, RedshiftLogger logger) {
    this(msg, state, cause);
    if(RedshiftLogger.isEnable())
    	logger.logError(this);
  }
  
  public RedshiftException(String msg, RedshiftState state, Throwable cause) {
    super(msg, state == null ? null : state.getState(), cause);
  }

  public RedshiftException(String msg, RedshiftState state) {
    super(msg, state == null ? null : state.getState());
  }

  public RedshiftException(String msg, Throwable cause) {
    super(msg, null , cause);
  }
  
  public RedshiftException(String msg) {
    super(msg, "");
  }
  
  public RedshiftException(ServerErrorMessage serverError) {
    this(serverError, true);
  }

  public RedshiftException(ServerErrorMessage serverError, boolean detail) {
    super(detail ? serverError.getExternalErrorMessage() : serverError.getNonSensitiveErrorMessage(), serverError.getSQLState());
    this.serverError = serverError;
  }

  public ServerErrorMessage getServerErrorMessage() {
    return serverError;
  }
  
  public SQLException getSQLException() {
  	return new SQLException(this.getMessage(),this.getSQLState(), this.getCause());
  }
}
