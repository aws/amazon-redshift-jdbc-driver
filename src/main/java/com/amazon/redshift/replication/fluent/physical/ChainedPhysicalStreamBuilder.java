/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.physical;

import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.fluent.ChainedCommonStreamBuilder;

import java.sql.SQLException;

public interface ChainedPhysicalStreamBuilder extends
    ChainedCommonStreamBuilder<ChainedPhysicalStreamBuilder> {

  /**
   * Open physical replication stream.
   *
   * @return not null RedshiftReplicationStream available for fetch wal logs in binary form
   * @throws SQLException on error
   */
  RedshiftReplicationStream start() throws SQLException;
}
