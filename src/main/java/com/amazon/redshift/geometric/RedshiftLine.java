/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.geometric;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftObject;
import com.amazon.redshift.util.RedshiftTokenizer;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements a line represented by the linear equation Ax + By + C = 0.
 **/
public class RedshiftLine extends RedshiftObject implements Serializable, Cloneable {

  /**
   * Coefficient of x.
   */
  public double a;

  /**
   * Coefficient of y.
   */
  public double b;

  /**
   * Constant.
   */
  public double c;

  /**
   * @param a coefficient of x
   * @param b coefficient of y
   * @param c constant
   */
  public RedshiftLine(double a, double b, double c) {
    this();
    this.a = a;
    this.b = b;
    this.c = c;
  }

  /**
   * @param x1 coordinate for first point on the line
   * @param y1 coordinate for first point on the line
   * @param x2 coordinate for second point on the line
   * @param y2 coordinate for second point on the line
   */
  public RedshiftLine(double x1, double y1, double x2, double y2) {
    this();
    if (x1 == x2) {
      a = -1;
      b = 0;
    } else {
      a = (y2 - y1) / (x2 - x1);
      b = -1;
    }
    c = y1 - a * x1;
  }

  /**
   * @param p1 first point on the line
   * @param p2 second point on the line
   */
  public RedshiftLine(RedshiftPoint p1, RedshiftPoint p2) {
    this(p1.x, p1.y, p2.x, p2.y);
  }

  /**
   * @param lseg Line segment which calls on this line.
   */
  public RedshiftLine(RedshiftLseg lseg) {
    this(lseg.point[0], lseg.point[1]);
  }

  /**
   * @param s definition of the line in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  public RedshiftLine(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * required by the driver.
   */
  public RedshiftLine() {
    setType("line");
  }

  /**
   * @param s Definition of the line in Redshift's syntax
   * @throws SQLException on conversion failure
   */
  @Override
  public void setValue(String s) throws SQLException {
    if (s.trim().startsWith("{")) {
      RedshiftTokenizer t = new RedshiftTokenizer(RedshiftTokenizer.removeCurlyBrace(s), ',');
      if (t.getSize() != 3) {
        throw new RedshiftException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
            RedshiftState.DATA_TYPE_MISMATCH);
      }
      a = Double.parseDouble(t.getToken(0));
      b = Double.parseDouble(t.getToken(1));
      c = Double.parseDouble(t.getToken(2));
    } else if (s.trim().startsWith("[")) {
      RedshiftTokenizer t = new RedshiftTokenizer(RedshiftTokenizer.removeBox(s), ',');
      if (t.getSize() != 2) {
        throw new RedshiftException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
            RedshiftState.DATA_TYPE_MISMATCH);
      }
      RedshiftPoint point1 = new RedshiftPoint(t.getToken(0));
      RedshiftPoint point2 = new RedshiftPoint(t.getToken(1));
      a = point2.x - point1.x;
      b = point2.y - point1.y;
      c = point1.y;
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two lines are identical
   */
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }

    RedshiftLine pGline = (RedshiftLine) obj;

    return Double.compare(pGline.a, a) == 0
        && Double.compare(pGline.b, b) == 0
        && Double.compare(pGline.c, c) == 0;
  }

  public int hashCode() {
    int result = super.hashCode();
    long temp;
    temp = Double.doubleToLongBits(a);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(b);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(c);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  /**
   * @return the RedshiftLine in the syntax expected by com.amazon.redshift
   */
  public String getValue() {
    return "{" + a + "," + b + "," + c + "}";
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    // squid:S2157 "Cloneables" should implement "clone
    return super.clone();
  }
}
