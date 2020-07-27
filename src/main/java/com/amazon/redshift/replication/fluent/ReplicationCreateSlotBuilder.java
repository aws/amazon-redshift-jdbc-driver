/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import com.amazon.redshift.replication.fluent.logical.LogicalCreateSlotBuilder;
import com.amazon.redshift.replication.fluent.physical.ChainedPhysicalCreateSlotBuilder;
import com.amazon.redshift.replication.fluent.physical.PhysicalCreateSlotBuilder;

public class ReplicationCreateSlotBuilder implements ChainedCreateReplicationSlotBuilder {
  private final BaseConnection baseConnection;

  public ReplicationCreateSlotBuilder(BaseConnection baseConnection) {
    this.baseConnection = baseConnection;
  }

  @Override
  public ChainedLogicalCreateSlotBuilder logical() {
    return new LogicalCreateSlotBuilder(baseConnection);
  }

  @Override
  public ChainedPhysicalCreateSlotBuilder physical() {
    return new PhysicalCreateSlotBuilder(baseConnection);
  }
}
