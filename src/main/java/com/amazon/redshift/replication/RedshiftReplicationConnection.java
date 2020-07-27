/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.replication.fluent.ChainedCreateReplicationSlotBuilder;
import com.amazon.redshift.replication.fluent.ChainedStreamBuilder;

import java.sql.SQLException;

/**
 * Api available only if connection was create with required for replication properties: {@link
 * RedshiftProperty#REPLICATION} and {@link RedshiftProperty#ASSUME_MIN_SERVER_VERSION}. Without it property
 * building replication stream fail with exception.
 */
public interface RedshiftReplicationConnection {

  /**
   * After start replication stream this connection not available to use for another queries until
   * replication stream will not close.
   *
   * @return not null fluent api for build replication stream
   */
  ChainedStreamBuilder replicationStream();

  /**
   * <p>Create replication slot, that can be next use in {@link RedshiftReplicationConnection#replicationStream()}</p>
   *
   * <p>Replication slots provide an automated way to ensure that the master does not remove WAL
   * segments until they have been received by all standbys, and that the master does not remove
   * rows which could cause a recovery conflict even when the standby is disconnected.</p>
   *
   * @return not null fluent api for build create replication slot
   */
  ChainedCreateReplicationSlotBuilder createReplicationSlot();

  /**
   * @param slotName not null replication slot name exists in database that should be drop
   * @throws SQLException if the replication slot cannot be dropped.
   */
  void dropReplicationSlot(String slotName) throws SQLException;
}
