/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.logical;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.replication.LogSequenceNumber;
import com.amazon.redshift.replication.ReplicationSlotInfo;
import com.amazon.redshift.replication.ReplicationType;
import com.amazon.redshift.replication.fluent.AbstractCreateSlotBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LogicalCreateSlotBuilder
    extends AbstractCreateSlotBuilder<ChainedLogicalCreateSlotBuilder>
    implements ChainedLogicalCreateSlotBuilder {

  private String outputPlugin;

  public LogicalCreateSlotBuilder(BaseConnection connection) {
    super(connection);
  }

  @Override
  protected ChainedLogicalCreateSlotBuilder self() {
    return this;
  }

  @Override
  public ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin) {
    this.outputPlugin = outputPlugin;
    return self();
  }

  @Override
  public ReplicationSlotInfo make() throws SQLException {
    if (outputPlugin == null || outputPlugin.isEmpty()) {
      throw new IllegalArgumentException(
          "OutputPlugin required parameter for logical replication slot");
    }

    if (slotName == null || slotName.isEmpty()) {
      throw new IllegalArgumentException("Replication slotName can't be null");
    }

    Statement statement = connection.createStatement();
    ResultSet result = null;
    ReplicationSlotInfo slotInfo = null;
    try {
      statement.execute(String.format(
          "CREATE_REPLICATION_SLOT %s %s LOGICAL %s",
          slotName,
          temporaryOption ? "TEMPORARY" : "",
          outputPlugin
      ));
      result = statement.getResultSet();
      if (result != null && result.next()) {
        slotInfo = new ReplicationSlotInfo(
            result.getString("slot_name"),
            ReplicationType.LOGICAL,
            LogSequenceNumber.valueOf(result.getString("consistent_point")),
            result.getString("snapshot_name"),
            result.getString("output_plugin"));
      }
    } finally {
      if (result != null) {
        result.close();
      }
      statement.close();
    }
    return slotInfo;
  }
}
