/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.core.v3.ConnectionFactoryImpl;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.HostSpec;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Handles protocol-specific connection setup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public abstract class ConnectionFactory {
  /**
   * <p>Establishes and initializes a new connection.</p>
   *
   * <p>If the "protocolVersion" property is specified, only that protocol version is tried. Otherwise,
   * all protocols are tried in order, falling back to older protocols as necessary.</p>
   *
   * <p>Currently, protocol versions 3 (7.4+) is supported.</p>
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param logger the logger to log the entry for debugging.       
   * @return the new, initialized, connection
   * @throws SQLException if the connection could not be established.
   */
  public static QueryExecutor openConnection(HostSpec[] hostSpecs, String user,
      String database, Properties info, RedshiftLogger logger) throws SQLException {
    String protoName = RedshiftProperty.PROTOCOL_VERSION.get(info);

    if (protoName == null || protoName.isEmpty() || "3".equals(protoName)) {
      ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
      QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(
          hostSpecs, user, database, info, logger);
      if (queryExecutor != null) {
        return queryExecutor;
      }
    }

    throw new RedshiftException(
        GT.tr("A connection could not be made using the requested protocol {0}.", protoName),
        RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
  }

  /**
   * Implementation of {@link #openConnection} for a particular protocol version. Implemented by
   * subclasses of {@link ConnectionFactory}.
   *
   * @param hostSpecs at least one host and port to connect to; multiple elements for round-robin
   *        failover
   * @param user the username to authenticate with; may not be null.
   * @param database the database on the server to connect to; may not be null.
   * @param info extra properties controlling the connection; notably, "password" if present
   *        supplies the password to authenticate with.
   * @param logger the logger to log the entry for debugging.       
   * @return the new, initialized, connection, or <code>null</code> if this protocol version is not
   *         supported by the server.
   * @throws SQLException if the connection could not be established for a reason other than
   *         protocol version incompatibility.
   */
  public abstract QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user,
      String database, Properties info, RedshiftLogger logger) throws SQLException;

  /**
   * Safely close the given stream.
   *
   * @param newStream The stream to close.
   */
  protected void closeStream(RedshiftStream newStream) {
    if (newStream != null) {
      try {
        newStream.close();
      } catch (IOException e) {
      }
    }
  }
}
