package com.amazon.redshift.util;

import java.io.Serializable;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Locale;
import com.amazon.redshift.util.RedshiftInterval;

public class RedshiftIntervalYearToMonth extends RedshiftInterval
                                         implements Serializable, Cloneable {

  /**
   * required by the driver.
   */
  public RedshiftIntervalYearToMonth() {
    setType("intervaly2m");
  }
  
  /**
   * Initialize an interval with a given interval string representation.
   * This method simply calls the parent public method setValue().
   *
   * @param value String representated interval (e.g. '3 years 2 mons').
   * @throws SQLException Is thrown if the string representation has an
   *                      unknown format.
   * @see #setValue(String)
   */
  public RedshiftIntervalYearToMonth(String value) throws SQLException {
    this();
    setValue(value);
    if (this.getDays() != 0 || this.getHours() != 0 || this.getMinutes() != 0 ||
        this.getWholeSeconds() != 0 || this.getMicroSeconds() != 0) {
      throw new RedshiftException("Invalid value for Interval Year To Month. " +
                                  "Value cannot contain day-time parts.");
    };
  }

  public RedshiftIntervalYearToMonth(int year, int month) throws SQLException {
    this();
    setValue(year * 12 + month, 0);
  }

  public RedshiftIntervalYearToMonth(int month) throws SQLException {
    this();
    setValue(month, 0);
  }

  /**
   * Set all values of this interval using just one specified value.
   * 
   * @param month Total number of months (assuming 12 months in a year)
   */
  public void setValue(int month) {
    super.setValue(month, 0);
  }

  /**
   * Override the parent setValue method disallowing a non-zero value for time.
   * 
   * @param month Total number of months (assuming 12 months in a year)
   * @param time  Should be 0.
   */
  @Override
  public void setValue(int month, long time) {
    assert(time == 0);
    super.setValue(month, 0);
  }

  /**
   * Override the parent setDays method disallowing a non-zero value.
   *
   * @param days Should be 0.
   */
  @Override
  public void setDays(int days) {
    assert(days == 0);
    super.setDays(0);
  }

  /**
   * Override the parent setHours method disallowing a non-zero value.
   *
   * @param hours Should be 0.
   */
  @Override
  public void setHours(int hours) {
    assert(hours == 0);
    super.setHours(0);
  }

  /**
   * Override the parent setMinutes method disallowing a non-zero value.
   *
   * @param minutes Should be 0.
   */
  @Override
  public void setMinutes(int minutes) {
    assert(minutes == 0);
    super.setMinutes(0);
  }

  /**
   * Override the parent setSeconds method disallowing a non-zero value.
   *
   * @param seconds Should be 0.
   */
  @Override
  public void setSeconds(double seconds) {
    assert(seconds == 0);
    super.setSeconds(0);
  }

  /**
   * Returns the stored interval information as a string.
   *
   * @return String represented interval
   */
  @Override
  public String getValue() {
    return String.format(
      Locale.ROOT,
      "%d years %d mons",
      this.getYears(),
      this.getMonths()
    );
  }

  /**
   * Add this interval's value to the passed interval. This is backwards to what I would expect, but
   * this makes it match the other existing add methods.
   *
   * @param interval intval to add
   */
  public void add(RedshiftIntervalYearToMonth interval) {
    interval.setValue(totalMonths() + interval.totalMonths());
  }

  /**
   * Returns whether an object is equal to this one or not.
   *
   * @param obj Object to compare with
   * @return true if the two intervals are identical
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof RedshiftIntervalYearToMonth)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    final RedshiftIntervalYearToMonth pgi = (RedshiftIntervalYearToMonth) obj;

    return pgi.getYears() == this.getYears() &&
           pgi.getMonths() == this.getMonths();
  }
}
