/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.replication.fluent.ChainedCreateReplicationSlotBuilder;
import com.amazon.redshift.replication.fluent.ChainedStreamBuilder;
import com.amazon.redshift.replication.fluent.ReplicationCreateSlotBuilder;
import com.amazon.redshift.replication.fluent.ReplicationStreamBuilder;

import java.sql.SQLException;
import java.sql.Statement;

public class RedshiftReplicationConnectionImpl implements RedshiftReplicationConnection {
  private BaseConnection connection;

  public RedshiftReplicationConnectionImpl(BaseConnection connection) {
    this.connection = connection;
  }

  @Override
  public ChainedStreamBuilder replicationStream() {
    return new ReplicationStreamBuilder(connection);
  }

  @Override
  public ChainedCreateReplicationSlotBuilder createReplicationSlot() {
    return new ReplicationCreateSlotBuilder(connection);
  }

  @Override
  public void dropReplicationSlot(String slotName) throws SQLException {
    if (slotName == null || slotName.isEmpty()) {
      throw new IllegalArgumentException("Replication slot name can't be null or empty");
    }

    Statement statement = connection.createStatement();
    try {
      statement.execute("DROP_REPLICATION_SLOT " + slotName);
    } finally {
      statement.close();
    }
  }
}
