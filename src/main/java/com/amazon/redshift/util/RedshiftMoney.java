/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a class that handles the Redshift money and cash types.
 */
public class RedshiftMoney extends RedshiftObject implements Serializable, Cloneable {
  /*
   * The value of the field
   */
  public double val;

  /**
   * @param value of field
   */
  public RedshiftMoney(double value) {
    this();
    val = value;
  }

  public RedshiftMoney(String value) throws SQLException {
    this();
    setValue(value);
  }

  /*
   * Required by the driver
   */
  public RedshiftMoney() {
    setType("money");
  }

  public void setValue(String s) throws SQLException {
    try {
      String s1;
      boolean negative;

      negative = (s.charAt(0) == '(');

      // Remove any () (for negative) & currency symbol
      s1 = RedshiftTokenizer.removePara(s).substring(1);

      // Strip out any , in currency
      int pos = s1.indexOf(',');
      while (pos != -1) {
        s1 = s1.substring(0, pos) + s1.substring(pos + 1);
        pos = s1.indexOf(',');
      }

      val = Double.parseDouble(s1);
      val = negative ? -val : val;

    } catch (NumberFormatException e) {
      throw new RedshiftException(GT.tr("Conversion of money failed."),
          RedshiftState.NUMERIC_CONSTANT_OUT_OF_RANGE, e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(val);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  public boolean equals(Object obj) {
    if (obj instanceof RedshiftMoney) {
      RedshiftMoney p = (RedshiftMoney) obj;
      return val == p.val;
    }
    return false;
  }

  public String getValue() {
    if (val < 0) {
      return "-$" + (-val);
    } else {
      return "$" + val;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // squid:S2157 "Cloneables" should implement "clone
    return super.clone();
  }
}
