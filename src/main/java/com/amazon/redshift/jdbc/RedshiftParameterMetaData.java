/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class RedshiftParameterMetaData implements ParameterMetaData {

  private final BaseConnection connection;
  private final int[] oids;

  public RedshiftParameterMetaData(BaseConnection connection, int[] oids) {
    this.connection = connection;
    this.oids = oids;
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    checkParamIndex(param);
    return connection.getTypeInfo().getJavaClass(oids[param - 1]);
  }

  @Override
  public int getParameterCount() {
    int rc = oids.length;

    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(false, rc);
    
    return rc;
  }

  /**
   * {@inheritDoc} For now report all parameters as inputs. CallableStatements may have one output,
   * but ignore that for now.
   */
  public int getParameterMode(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterModeIn;
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    checkParamIndex(param);
    return connection.getTypeInfo().getSQLType(oids[param - 1]);
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    checkParamIndex(param);
    return connection.getTypeInfo().getRSType(oids[param - 1]);
  }

  // we don't know this
  public int getPrecision(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we don't know this
  public int getScale(int param) throws SQLException {
    checkParamIndex(param);
    return 0;
  }

  // we can't tell anything about nullability
  public int isNullable(int param) throws SQLException {
    checkParamIndex(param);
    return ParameterMetaData.parameterNullableUnknown;
  }

  /**
   * {@inheritDoc} Redshift doesn't have unsigned numbers
   */
  @Override
  public boolean isSigned(int param) throws SQLException {
    checkParamIndex(param);
    return connection.getTypeInfo().isSigned(oids[param - 1]);
  }

  private void checkParamIndex(int param) throws RedshiftException {
    if (param < 1 || param > oids.length) {
      throw new RedshiftException(
          GT.tr("The parameter index is out of range: {0}, number of parameters: {1}.",
              param, oids.length),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }
}
