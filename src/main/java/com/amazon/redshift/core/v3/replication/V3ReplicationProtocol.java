/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core.v3.replication;

import com.amazon.redshift.copy.CopyDual;
import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.core.QueryExecutor;
import com.amazon.redshift.core.ReplicationProtocol;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.ReplicationType;
import com.amazon.redshift.replication.fluent.CommonOptions;
import com.amazon.redshift.replication.fluent.logical.LogicalReplicationOptions;
import com.amazon.redshift.replication.fluent.physical.PhysicalReplicationOptions;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class V3ReplicationProtocol implements ReplicationProtocol {

  private RedshiftLogger logger;
  private final QueryExecutor queryExecutor;
  private final RedshiftStream pgStream;

  public V3ReplicationProtocol(QueryExecutor queryExecutor, RedshiftStream pgStream) {
    this.queryExecutor = queryExecutor;
    this.pgStream = pgStream;
  }

  public RedshiftReplicationStream startLogical(LogicalReplicationOptions options, RedshiftLogger logger)
      throws SQLException {

    String query = createStartLogicalQuery(options);
    return initializeReplication(query, options, ReplicationType.LOGICAL, logger);
  }

  public RedshiftReplicationStream startPhysical(PhysicalReplicationOptions options, RedshiftLogger logger)
      throws SQLException {

    String query = createStartPhysicalQuery(options);
    return initializeReplication(query, options, ReplicationType.PHYSICAL, logger);
  }

  private RedshiftReplicationStream initializeReplication(String query, CommonOptions options,
      ReplicationType replicationType, RedshiftLogger logger)
      throws SQLException {
  	this.logger = logger;
  	if(RedshiftLogger.isEnable())
  		this.logger.log(LogLevel.DEBUG, " FE=> StartReplication(query: {0})", query);

    configureSocketTimeout(options);
    CopyDual copyDual = (CopyDual) queryExecutor.startCopy(query, true);

    return new V3RedshiftReplicationStream(
        copyDual,
        options.getStartLSNPosition(),
        options.getStatusInterval(),
        replicationType,
        logger
    );
  }

  /**
   * START_REPLICATION [SLOT slot_name] [PHYSICAL] XXX/XXX.
   */
  private String createStartPhysicalQuery(PhysicalReplicationOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("START_REPLICATION");

    if (options.getSlotName() != null) {
      builder.append(" SLOT ").append(options.getSlotName());
    }

    builder.append(" PHYSICAL ").append(options.getStartLSNPosition().asString());

    return builder.toString();
  }

  /**
   * START_REPLICATION SLOT slot_name LOGICAL XXX/XXX [ ( option_name [option_value] [, ... ] ) ]
   */
  private String createStartLogicalQuery(LogicalReplicationOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("START_REPLICATION SLOT ")
        .append(options.getSlotName())
        .append(" LOGICAL ")
        .append(options.getStartLSNPosition().asString());

    Properties slotOptions = options.getSlotOptions();
    if (slotOptions.isEmpty()) {
      return builder.toString();
    }

    //todo replace on java 8
    builder.append(" (");
    boolean isFirst = true;
    for (String name : slotOptions.stringPropertyNames()) {
      if (isFirst) {
        isFirst = false;
      } else {
        builder.append(", ");
      }
      builder.append('\"').append(name).append('\"').append(" ")
          .append('\'').append(slotOptions.getProperty(name)).append('\'');
    }
    builder.append(")");

    return builder.toString();
  }

  private void configureSocketTimeout(CommonOptions options) throws RedshiftException {
    if (options.getStatusInterval() == 0) {
      return;
    }

    try {
      int previousTimeOut = pgStream.getSocket().getSoTimeout();

      int minimalTimeOut;
      if (previousTimeOut > 0) {
        minimalTimeOut = Math.min(previousTimeOut, options.getStatusInterval());
      } else {
        minimalTimeOut = options.getStatusInterval();
      }

      pgStream.getSocket().setSoTimeout(minimalTimeOut);
      // Use blocking 1ms reads for `available()` checks
      pgStream.setMinStreamAvailableCheckDelay(0);
    } catch (IOException ioe) {
      throw new RedshiftException(GT.tr("The connection attempt failed."),
          RedshiftState.CONNECTION_UNABLE_TO_CONNECT, ioe);
    }
  }
}
