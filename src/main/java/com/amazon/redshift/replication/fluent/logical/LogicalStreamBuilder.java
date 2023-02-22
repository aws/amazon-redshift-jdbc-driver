/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.logical;

import com.amazon.redshift.replication.LogSequenceNumber;
import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.fluent.AbstractStreamBuilder;
import com.amazon.redshift.util.RedshiftProperties;

import java.sql.SQLException;
import java.util.Properties;

public class LogicalStreamBuilder extends AbstractStreamBuilder<ChainedLogicalStreamBuilder>
    implements ChainedLogicalStreamBuilder, LogicalReplicationOptions {
  private final Properties slotOptions;

  private StartLogicalReplicationCallback startCallback;

  /**
   * @param startCallback not null callback that should be execute after build parameters for start
   *                      replication
   */
  public LogicalStreamBuilder(StartLogicalReplicationCallback startCallback) {
    this.startCallback = startCallback;
    this.slotOptions = new Properties();
  }

  @Override
  protected ChainedLogicalStreamBuilder self() {
    return this;
  }

  @Override
  public RedshiftReplicationStream start() throws SQLException {
    return startCallback.start(this);
  }

  @Override
  public String getSlotName() {
    return slotName;
  }

  @Override
  public ChainedLogicalStreamBuilder withStartPosition(LogSequenceNumber lsn) {
    startPosition = lsn;
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, boolean optionValue) {
    slotOptions.setProperty(optionName, String.valueOf(optionValue));
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, int optionValue) {
    slotOptions.setProperty(optionName, String.valueOf(optionValue));
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOption(String optionName, String optionValue) {
    slotOptions.setProperty(optionName, optionValue);
    return this;
  }

  @Override
  public ChainedLogicalStreamBuilder withSlotOptions(Properties options) {
    for (String propertyName : options.stringPropertyNames()) {
      slotOptions.setProperty(propertyName, options.getProperty(propertyName));
    }
    return this;
  }

  @Override
  public LogSequenceNumber getStartLSNPosition() {
    return startPosition;
  }

  @Override
  public Properties getSlotOptions() {
    return slotOptions;
  }

  @Override
  public int getStatusInterval() {
    return statusIntervalMs;
  }
}
