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
 * This implements a path (a multiple segmented line, which may be closed).
 */
public class RedshiftPath extends RedshiftObject implements Serializable, Cloneable {
  /**
   * True if the path is open, false if closed.
   */
  public boolean open;

  /**
   * The points defining this path.
   */
  public RedshiftPoint[] points;

  /**
   * @param points the RedshiftPoints that define the path
   * @param open True if the path is open, false if closed
   */
  public RedshiftPath(RedshiftPoint[] points, boolean open) {
    this();
    this.points = points;
    this.open = open;
  }

  /**
   * Required by the driver.
   */
  public RedshiftPath() {
    setType("path");
  }

  /**
   * @param s definition of the path in Redshift's syntax.
   * @throws SQLException on conversion failure
   */
  public RedshiftPath(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * @param s Definition of the path in Redshift's syntax
   * @throws SQLException on conversion failure
   */
  public void setValue(String s) throws SQLException {
    // First test to see if were open
    if (s.startsWith("[") && s.endsWith("]")) {
      open = true;
      s = RedshiftTokenizer.removeBox(s);
    } else if (s.startsWith("(") && s.endsWith(")")) {
      open = false;
      s = RedshiftTokenizer.removePara(s);
    } else {
      throw new RedshiftException(GT.tr("Cannot tell if path is open or closed: {0}.", s),
          RedshiftState.DATA_TYPE_MISMATCH);
    }

    RedshiftTokenizer t = new RedshiftTokenizer(s, ',');
    int npoints = t.getSize();
    points = new RedshiftPoint[npoints];
    for (int p = 0; p < npoints; p++) {
      points[p] = new RedshiftPoint(t.getToken(p));
    }
  }

  /**
   * @param obj Object to compare with
   * @return true if the two paths are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof RedshiftPath) {
      RedshiftPath p = (RedshiftPath) obj;

      if (p.points.length != points.length) {
        return false;
      }

      if (p.open != open) {
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
    RedshiftPath newRSpath = (RedshiftPath) super.clone();
    if (newRSpath.points != null) {
      newRSpath.points = (RedshiftPoint[]) newRSpath.points.clone();
      for (int i = 0; i < newRSpath.points.length; ++i) {
        newRSpath.points[i] = (RedshiftPoint) newRSpath.points[i].clone();
      }
    }
    return newRSpath;
  }

  /**
   * This returns the path in the syntax expected by com.amazon.redshift.
   */
  public String getValue() {
    StringBuilder b = new StringBuilder(open ? "[" : "(");

    for (int p = 0; p < points.length; p++) {
      if (p > 0) {
        b.append(",");
      }
      b.append(points[p].toString());
    }
    b.append(open ? "]" : ")");

    return b.toString();
  }

  public boolean isOpen() {
    return open;
  }

  public boolean isClosed() {
    return !open;
  }

  public void closePath() {
    open = false;
  }

  public void openPath() {
    open = true;
  }
}
