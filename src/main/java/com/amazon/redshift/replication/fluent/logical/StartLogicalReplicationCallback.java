/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.logical;

import com.amazon.redshift.replication.RedshiftReplicationStream;

import java.sql.SQLException;

public interface StartLogicalReplicationCallback {
  RedshiftReplicationStream start(LogicalReplicationOptions options) throws SQLException;
}
