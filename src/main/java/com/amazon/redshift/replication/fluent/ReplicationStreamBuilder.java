/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.core.ReplicationProtocol;
import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.fluent.logical.ChainedLogicalStreamBuilder;
import com.amazon.redshift.replication.fluent.logical.LogicalReplicationOptions;
import com.amazon.redshift.replication.fluent.logical.LogicalStreamBuilder;
import com.amazon.redshift.replication.fluent.logical.StartLogicalReplicationCallback;
import com.amazon.redshift.replication.fluent.physical.ChainedPhysicalStreamBuilder;
import com.amazon.redshift.replication.fluent.physical.PhysicalReplicationOptions;
import com.amazon.redshift.replication.fluent.physical.PhysicalStreamBuilder;
import com.amazon.redshift.replication.fluent.physical.StartPhysicalReplicationCallback;

import java.sql.SQLException;

public class ReplicationStreamBuilder implements ChainedStreamBuilder {
  private final BaseConnection baseConnection;

  /**
   * @param connection not null connection with that will be associate replication
   */
  public ReplicationStreamBuilder(final BaseConnection connection) {
    this.baseConnection = connection;
  }

  @Override
  public ChainedLogicalStreamBuilder logical() {
    return new LogicalStreamBuilder(new StartLogicalReplicationCallback() {
      @Override
      public RedshiftReplicationStream start(LogicalReplicationOptions options) throws SQLException {
        ReplicationProtocol protocol = baseConnection.getReplicationProtocol();
        return protocol.startLogical(options, baseConnection.getLogger());
      }
    });
  }

  @Override
  public ChainedPhysicalStreamBuilder physical() {
    return new PhysicalStreamBuilder(new StartPhysicalReplicationCallback() {
      @Override
      public RedshiftReplicationStream start(PhysicalReplicationOptions options) throws SQLException {
        ReplicationProtocol protocol = baseConnection.getReplicationProtocol();
        return protocol.startPhysical(options, baseConnection.getLogger());
      }
    });
  }
}
