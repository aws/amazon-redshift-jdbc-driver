/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.physical;

import com.amazon.redshift.replication.RedshiftReplicationStream;

import java.sql.SQLException;

public interface StartPhysicalReplicationCallback {
  RedshiftReplicationStream start(PhysicalReplicationOptions options) throws SQLException;
}
