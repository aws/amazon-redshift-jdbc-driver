/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.fluent.logical.LogicalReplicationOptions;
import com.amazon.redshift.replication.fluent.physical.PhysicalReplicationOptions;

import java.sql.SQLException;

/**
 * <p>Abstracts the protocol-specific details of physic and logic replication.</p>
 *
 * <p>With each connection open with replication options associate own instance ReplicationProtocol.</p>
 */
public interface ReplicationProtocol {
  /**
   * @param options not null options for logical replication stream
   * @return not null stream instance from which available fetch wal logs that was decode by output
   *     plugin
   * @throws SQLException on error
   */
  RedshiftReplicationStream startLogical(LogicalReplicationOptions options, RedshiftLogger logger) throws SQLException;

  /**
   * @param options not null options for physical replication stream
   * @return not null stream instance from which available fetch wal logs
   * @throws SQLException on error
   */
  RedshiftReplicationStream startPhysical(PhysicalReplicationOptions options, RedshiftLogger logger) throws SQLException;
}
