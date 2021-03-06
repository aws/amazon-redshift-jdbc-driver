/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.geometric;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftBinaryObject;
import com.amazon.redshift.util.RedshiftObject;
import com.amazon.redshift.util.RedshiftTokenizer;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * This represents the box datatype within com.amazon.redshift.
 */
public class RedshiftBox extends RedshiftObject implements RedshiftBinaryObject, Serializable, Cloneable {
  /**
   * These are the two points.
   */
  public RedshiftPoint[] point = new RedshiftPoint[2];

  /**
   * @param x1 first x coordinate
   * @param y1 first y coordinate
   * @param x2 second x coordinate
   * @param y2 second y coordinate
   */
  public RedshiftBox(double x1, double y1, double x2, double y2) {
    this();
    this.point[0] = new RedshiftPoint(x1, y1);
    this.point[1] = new RedshiftPoint(x2, y2);
  }

  /**
   * @param p1 first point
   * @param p2 second point
   */
  public RedshiftBox(RedshiftPoint p1, RedshiftPoint p2) {
    this();
    this.point[0] = p1;
    this.point[1] = p2;
  }

  /**
   * @param s Box definition in Redshift syntax
   * @throws SQLException if definition is invalid
   */
  public RedshiftBox(String s) throws SQLException {
    this();
    setValue(s);
  }

  /**
   * Required constructor.
   */
  public RedshiftBox() {
    setType("box");
  }

  /**
   * This method sets the value of this object. It should be overidden, but still called by
   * subclasses.
   *
   * @param value a string representation of the value of the object
   * @throws SQLException thrown if value is invalid for this type
   */
  @Override
  public void setValue(String value) throws SQLException {
    RedshiftTokenizer t = new RedshiftTokenizer(value, ',');
    if (t.getSize() != 2) {
      throw new RedshiftException(
          GT.tr("Conversion to type {0} failed: {1}.", type, value),
          RedshiftState.DATA_TYPE_MISMATCH);
    }

    point[0] = new RedshiftPoint(t.getToken(0));
    point[1] = new RedshiftPoint(t.getToken(1));
  }

  /**
   * @param b Definition of this point in Redshift's binary syntax
   */
  public void setByteValue(byte[] b, int offset) {
    point[0] = new RedshiftPoint();
    point[0].setByteValue(b, offset);
    point[1] = new RedshiftPoint();
    point[1].setByteValue(b, offset + point[0].lengthInBytes());
  }

  /**
   * @param obj Object to compare with
   * @return true if the two boxes are identical
   */
  public boolean equals(Object obj) {
    if (obj instanceof RedshiftBox) {
      RedshiftBox p = (RedshiftBox) obj;

      // Same points.
      if (p.point[0].equals(point[0]) && p.point[1].equals(point[1])) {
        return true;
      }

      // Points swapped.
      if (p.point[0].equals(point[1]) && p.point[1].equals(point[0])) {
        return true;
      }

      // Using the opposite two points of the box:
      // (x1,y1),(x2,y2) -> (x1,y2),(x2,y1)
      if (p.point[0].x == point[0].x && p.point[0].y == point[1].y
          && p.point[1].x == point[1].x && p.point[1].y == point[0].y) {
        return true;
      }

      // Using the opposite two points of the box, and the points are swapped
      // (x1,y1),(x2,y2) -> (x2,y1),(x1,y2)
      if (p.point[0].x == point[1].x && p.point[0].y == point[0].y
          && p.point[1].x == point[0].x && p.point[1].y == point[1].y) {
        return true;
      }
    }

    return false;
  }

  public int hashCode() {
    // This relies on the behaviour of point's hashcode being an exclusive-OR of
    // its X and Y components; we end up with an exclusive-OR of the two X and
    // two Y components, which is equal whenever equals() would return true
    // since xor is commutative.
    return point[0].hashCode() ^ point[1].hashCode();
  }

  public Object clone() throws CloneNotSupportedException {
    RedshiftBox newRSbox = (RedshiftBox) super.clone();
    if (newRSbox.point != null) {
      newRSbox.point = newRSbox.point.clone();
      for (int i = 0; i < newRSbox.point.length; ++i) {
        if (newRSbox.point[i] != null) {
          newRSbox.point[i] = (RedshiftPoint) newRSbox.point[i].clone();
        }
      }
    }
    return newRSbox;
  }

  /**
   * @return the RedshiftBox in the syntax expected by com.amazon.redshift
   */
  public String getValue() {
    return point[0].toString() + "," + point[1].toString();
  }

  public int lengthInBytes() {
    return point[0].lengthInBytes() + point[1].lengthInBytes();
  }

  public void toBytes(byte[] bytes, int offset) {
    point[0].toBytes(bytes, offset);
    point[1].toBytes(bytes, offset + point[0].lengthInBytes());
  }
}
