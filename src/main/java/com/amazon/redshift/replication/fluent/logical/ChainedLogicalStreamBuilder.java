/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.replication.fluent.logical;

import com.amazon.redshift.replication.RedshiftReplicationStream;
import com.amazon.redshift.replication.fluent.ChainedCommonStreamBuilder;

import java.sql.SQLException;
import java.util.Properties;

public interface ChainedLogicalStreamBuilder
    extends ChainedCommonStreamBuilder<ChainedLogicalStreamBuilder> {
  /**
   * Open logical replication stream.
   *
   * @return not null RedshfitReplicationStream available for fetch data in logical form
   * @throws SQLException  if there are errors
   */
  RedshiftReplicationStream start() throws SQLException;

  /**
   *
   * @param optionName name of option
   * @param optionValue boolean value
   * @return ChainedLogicalStreamBuilder
   */

  ChainedLogicalStreamBuilder withSlotOption(String optionName, boolean optionValue);

  /**
   *
   * @param optionName name of option
   * @param optionValue integer value
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOption(String optionName, int optionValue);

  /**
   *
   * @param optionName name of option
   * @param optionValue String value
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOption(String optionName, String optionValue);

  /**
   *
   * @param options properties
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOptions(Properties options);

}
