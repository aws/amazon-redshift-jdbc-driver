/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.sql.PreparedStatement;
import java.sql.Time;
import java.util.Calendar;

/**
 * This class augments the Java built-in Time to allow for explicit setting of the time zone.
 */
public class RedshiftTime extends Time {
  /**
   * The serial version UID.
   */
  private static final long serialVersionUID = 3592492258676494276L;

  /**
   * The optional calendar for this time.
   */
  private Calendar calendar;
  
  private int nanos;

  /**
   * Constructs a <code>RedshiftTime</code> without a time zone.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT; a negative number is milliseconds
   *        before January 1, 1970, 00:00:00 GMT.
   * @see Time#Time(long)
   */
  public RedshiftTime(long time) {
    this(time, null);
  }

  public RedshiftTime(long time, int nanos) {
    this(time, null, nanos);
  }
  
  /**
   * Constructs a <code>RedshiftTime</code> with the given calendar object. The calendar object is
   * optional. If absent, the driver will treat the time as <code>time without time zone</code>.
   * When present, the driver will treat the time as a <code>time with time zone</code> using the
   * <code>TimeZone</code> in the calendar object. Furthermore, this calendar will be used instead
   * of the calendar object passed to {@link PreparedStatement#setTime(int, Time, Calendar)}.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT; a negative number is milliseconds
   *        before January 1, 1970, 00:00:00 GMT.
   * @param calendar the calendar object containing the time zone or <code>null</code>.
   * @see Time#Time(long)
   */
  public RedshiftTime(long time, Calendar calendar) {
    this(time, calendar, 0);
  }

  /**
   * Store time with nanos.
   * 
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT; a negative number is milliseconds
   *        before January 1, 1970, 00:00:00 GMT.
   * @param calendar the calendar object containing the time zone or <code>null</code>.
   * @param nanos nanos
   */
  public RedshiftTime(long time, Calendar calendar, int nanos) {
    super(time);
    this.setCalendar(calendar);
    this.nanos = nanos;
  }

  public RedshiftTime(Time time, int nanos) {
    this(time.getTime(), null, nanos);
  }
  
  /**
   * Sets the calendar object for this time.
   *
   * @param calendar the calendar object or <code>null</code>.
   */
  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  /**
   * Returns the calendar object for this time.
   *
   * @return the calendar or <code>null</code>.
   */
  public Calendar getCalendar() {
    return calendar;
  }

  /**
   * Returns nano seconds of time.
   * 
   * @return nanos
   */
  public int getNanos() {
    return nanos;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((calendar == null) ? 0 : calendar.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof RedshiftTime)) {
      return false;
    }
    RedshiftTime other = (RedshiftTime) obj;
    if (calendar == null) {
      if (other.calendar != null) {
        return false;
      }
    } else if (!calendar.equals(other.calendar)) {
      return false;
    }
    return true;
  }

  @Override
  public Object clone() {
    RedshiftTime clone = (RedshiftTime) super.clone();
    if (getCalendar() != null) {
      clone.setCalendar((Calendar) getCalendar().clone());
    }
    return clone;
  }
}
