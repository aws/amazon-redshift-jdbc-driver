/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.geometric;

import com.amazon.redshift.util.RedshiftObject;
import com.amazon.redshift.util.RedshiftTokenizer;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This implements the polygon datatype within Redshift.
 */
public class RedshiftPolygon extends RedshiftObject implements Serializable, Cloneable {
  /**
   * The points defining the polygon.
   */
  public RedshiftPoint[] points;

  /**
   * Creates a polygon using an array of RedshiftPoints.
   *
   * @param points the points defining the polygon
   */
  public RedshiftPolygon(RedshiftPoint[] points) {
    this();
    this.points = points;
  }

  /**
   * @param s definition of the polygon in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  public RedshiftPolygon(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * Required by the driver.
   */
  public RedshiftPolygon() {
    setType("polygon");
  }

  /**
   * @param s Definition of the polygon in Redshift's syntax
   * @throws SQLException on conversion failure
   */
  public void setValue(String s) throws SQLException {
    RedshiftTokenizer t = new RedshiftTokenizer(RedshiftTokenizer.removePara(s), ',');
    int npoints = t.getSize();
    points = new RedshiftPoint[npoints];
    for (int p = 0; p < npoints; p++) {
      points[p] = new RedshiftPoint(t.getToken(p));
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two polygons are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof RedshiftPolygon) {
      RedshiftPolygon p = (RedshiftPolygon) obj;

      if (p.points.length != points.length) {
        return false;
      }

      for (int i = 0; i < points.length; i++) {
        if (!points[i].equals(p.points[i])) {
          return false;
        }
      }

      return true;
    }
    return false;
  }

  public int hashCode() {
    // XXX not very good..
    int hash = 0;
    for (int i = 0; i < points.length && i < 5; ++i) {
      hash = hash ^ points[i].hashCode();
    }
    return hash;
  }

  public Object clone() throws CloneNotSupportedException {
    RedshiftPolygon newRSpolygon = (RedshiftPolygon) super.clone();
    if (newRSpolygon.points != null) {
      newRSpolygon.points = (RedshiftPoint[]) newRSpolygon.points.clone();
      for (int i = 0; i < newRSpolygon.points.length; ++i) {
        if (newRSpolygon.points[i] != null) {
          newRSpolygon.points[i] = (RedshiftPoint) newRSpolygon.points[i].clone();
        }
      }
    }
    return newRSpolygon;
  }

  /**
   * @return the RedshiftPolygon in the syntax expected by com.amazon.redshift
   */
  public String getValue() {
    StringBuilder b = new StringBuilder();
    b.append("(");
    for (int p = 0; p < points.length; p++) {
      if (p > 0) {
        b.append(",");
      }
      b.append(points[p].toString());
    }
    b.append(")");
    return b.toString();
  }
}
