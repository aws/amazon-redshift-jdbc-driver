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
 * This represents com.amazon.redshift's circle datatype, consisting of a point and a radius.
 */
public class RedshiftCircle extends RedshiftObject implements Serializable, Cloneable {
  /**
   * This is the center point.
   */
  public RedshiftPoint center;

  /**
   * This is the radius.
   */
  public double radius;

  /**
   * @param x coordinate of center
   * @param y coordinate of center
   * @param r radius of circle
   */
  public RedshiftCircle(double x, double y, double r) {
    this(new RedshiftPoint(x, y), r);
  }

  /**
   * @param c RedshiftPoint describing the circle's center
   * @param r radius of circle
   */
  public RedshiftCircle(RedshiftPoint c, double r) {
    this();
    this.center = c;
    this.radius = r;
  }

  /**
   * @param s definition of the circle in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  public RedshiftCircle(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * This constructor is used by the driver.
   */
  public RedshiftCircle() {
    setType("circle");
  }

  /**
   * @param s definition of the circle in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  @Override
  public void setValue(String s) throws SQLException {
    RedshiftTokenizer t = new RedshiftTokenizer(RedshiftTokenizer.removeAngle(s), ',');
    if (t.getSize() != 2) {
      throw new RedshiftException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
          RedshiftState.DATA_TYPE_MISMATCH);
    }

    try {
      center = new RedshiftPoint(t.getToken(0));
      radius = Double.parseDouble(t.getToken(1));
    } catch (NumberFormatException e) {
      throw new RedshiftException(GT.tr("Conversion to type {0} failed: {1}.", type, s),
          RedshiftState.DATA_TYPE_MISMATCH, e);
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two circles are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof RedshiftCircle) {
      RedshiftCircle p = (RedshiftCircle) obj;
      return p.center.equals(center) && p.radius == radius;
    }
    return false;
  }

  public int hashCode() {
    long v = Double.doubleToLongBits(radius);
    return (int) (center.hashCode() ^ v ^ (v >>> 32));
  }

  public Object clone() throws CloneNotSupportedException {
    RedshiftCircle newRScircle = (RedshiftCircle) super.clone();
    if (newRScircle.center != null) {
      newRScircle.center = (RedshiftPoint) newRScircle.center.clone();
    }
    return newRScircle;
  }

  /**
   * @return the RedshiftCircle in the syntax expected by com.amazon.redshift
   */
  public String getValue() {
    return "<" + center + "," + radius + ">";
  }
}
