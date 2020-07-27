/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.core.Utils;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.sql.SQLException;
import java.sql.Savepoint;

public class RedshiftSavepoint implements Savepoint {

  private boolean isValid;
  private final boolean isNamed;
  private int id;
  private String name;

  public RedshiftSavepoint(int id) {
    this.isValid = true;
    this.isNamed = false;
    this.id = id;
  }

  public RedshiftSavepoint(String name) {
    this.isValid = true;
    this.isNamed = true;
    this.name = name;
  }

  @Override
  public int getSavepointId() throws SQLException {
    if (!isValid) {
      throw new RedshiftException(GT.tr("Cannot reference a savepoint after it has been released."),
          RedshiftState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (isNamed) {
      throw new RedshiftException(GT.tr("Cannot retrieve the id of a named savepoint."),
          RedshiftState.WRONG_OBJECT_TYPE);
    }

    return id;
  }

  @Override
  public String getSavepointName() throws SQLException {
    if (!isValid) {
      throw new RedshiftException(GT.tr("Cannot reference a savepoint after it has been released."),
          RedshiftState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (!isNamed) {
      throw new RedshiftException(GT.tr("Cannot retrieve the name of an unnamed savepoint."),
          RedshiftState.WRONG_OBJECT_TYPE);
    }

    return name;
  }

  public void invalidate() {
    isValid = false;
  }

  public String getRSName() throws SQLException {
    if (!isValid) {
      throw new RedshiftException(GT.tr("Cannot reference a savepoint after it has been released."),
          RedshiftState.INVALID_SAVEPOINT_SPECIFICATION);
    }

    if (isNamed) {
      // We need to quote and escape the name in case it
      // contains spaces/quotes/etc.
      //
      return Utils.escapeIdentifier(null, name).toString();
    }

    return "JDBC_SAVEPOINT_" + id;
  }
}
