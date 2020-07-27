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
 * This implements a lseg (line segment) consisting of two points.
 */
public class RedshiftLseg extends RedshiftObject implements Serializable, Cloneable {
  /**
   * These are the two points.
   */
  public RedshiftPoint[] point = new RedshiftPoint[2];

  /**
   * @param x1 coordinate for first point
   * @param y1 coordinate for first point
   * @param x2 coordinate for second point
   * @param y2 coordinate for second point
   */
  public RedshiftLseg(double x1, double y1, double x2, double y2) {
    this(new RedshiftPoint(x1, y1), new RedshiftPoint(x2, y2));
  }

  /**
   * @param p1 first point
   * @param p2 second point
   */
  public RedshiftLseg(RedshiftPoint p1, RedshiftPoint p2) {
    this();
    this.point[0] = p1;
    this.point[1] = p2;
  }

  /**
   * @param s definition of the line segment in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  public RedshiftLseg(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * required by the driver.
   */
  public RedshiftLseg() {
    setType("lseg");
  }

  /**
   * @param s Definition of the line segment in Redshift's syntax
   * @throws SQLException on conversion failure
   */
  @Override
  public void setValue(String s) throws SQLException {
    RedshiftTokenizer t = new RedshiftTokenizer(RedshiftTokenizer.removeBox(s), ',');
    if (t.getSize() != 2) {
      throw new RedshiftException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
          RedshiftState.DATA_TYPE_MISMATCH);
    }

    point[0] = new RedshiftPoint(t.getToken(0));
    point[1] = new RedshiftPoint(t.getToken(1));
  }

  /**
   * @param obj Object to compare with
   * @return true if the two line segments are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof RedshiftLseg) {
      RedshiftLseg p = (RedshiftLseg) obj;
      return (p.point[0].equals(point[0]) && p.point[1].equals(point[1]))
          || (p.point[0].equals(point[1]) && p.point[1].equals(point[0]));
    }
    return false;
  }

  public int hashCode() {
    return point[0].hashCode() ^ point[1].hashCode();
  }

  public Object clone() throws CloneNotSupportedException {
    RedshiftLseg newRSlseg = (RedshiftLseg) super.clone();
    if (newRSlseg.point != null) {
      newRSlseg.point = (RedshiftPoint[]) newRSlseg.point.clone();
      for (int i = 0; i < newRSlseg.point.length; ++i) {
        if (newRSlseg.point[i] != null) {
          newRSlseg.point[i] = (RedshiftPoint) newRSlseg.point[i].clone();
        }
      }
    }
    return newRSlseg;
  }

  /**
   * @return the RedshiftLseg in the syntax expected by com.amazon.redshift
   */
  public String getValue() {
    return "[" + point[0] + "," + point[1] + "]";
  }
}
