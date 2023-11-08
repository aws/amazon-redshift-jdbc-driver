package com.amazon.redshift.util;

import java.io.Serializable;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.text.NumberFormat;
import com.amazon.redshift.util.RedshiftInterval;

public class RedshiftIntervalDayToSecond extends RedshiftInterval
                                         implements Serializable, Cloneable {

  /**
   * required by the driver.
   */
  public RedshiftIntervalDayToSecond() {
    setType("intervald2s");
  }
  
  /**
   * Initialize an interval with a given interval string representation.
   * This method simply calls the parent public method setValue().
   *
   * @param value String representated interval
   *              (e.g. '3 days 4 hours 1 mins 4.30 seconds').
   * @throws SQLException Is thrown if the string representation has an
   *                      unknown format.
   * @see #setValue(String)
   */
  public RedshiftIntervalDayToSecond(String value) throws SQLException {
    this();
    setValue(value);
    if (this.getYears() != 0 || this.getMonths() != 0) {
      throw new RedshiftException("Invalid value for Interval Day To Second. " +
                                  "Value cannot contain year-month parts.");
    };
  }

  public RedshiftIntervalDayToSecond(int days, int hours, int minutes,
                                     double seconds) throws SQLException {
    this();
    super.setValue(0 /* years */, 0 /* months */, days, hours, minutes, seconds);
    setValue(totalMicroseconds());
  }

  public RedshiftIntervalDayToSecond(long time) throws SQLException {
    this();
    setValue(0, time);
  }

  /**
   * Set all values of this interval using just one specified value.
   * 
   * @param time Total number of microseconds
   *      (assuming 1day = 24hrs = 1440mins = 86400secs = 8.64e10microsecs)
   */
  public void setValue(long time) {
    super.setValue(0, time);
  }

  /**
   * Override the parent setValue method disallowing a non-zero value for month.
   * 
   * @param month Should be 0.
   * @param time  Total number of microseconds
   *      (assuming 1day = 24hrs = 1440mins = 86400secs = 8.64e10microsecs)
   */
  @Override
  public void setValue(int month, long time) {
    assert(month == 0);
    super.setValue(0, time);
  }

  /**
   * Override the parent setYears method disallowing a non-zero value.
   *
   * @param years Should be 0.
   */
  @Override
  public void setYears(int years) {
    assert(years == 0);
    super.setYears(0);
  }

  /**
   * Override the parent setMonths method disallowing a non-zero value.
   *
   * @param months Should be 0.
   */
  @Override
  public void setMonths(int months) {
    assert(months == 0);
    super.setMonths(0);
  }

  /**
   * Returns the stored interval information as a string.
   *
   * @return String represented interval
   */
  @Override
  public String getValue() {
    DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
    df.applyPattern("0.0#####");

    return String.format(
      Locale.ROOT,
      "%d days %d hours %d mins %s secs",
      this.getDays(),
      this.getHours(),
      this.getMinutes(),
      df.format(this.getSeconds())
    );
  }

  /**
   * Add this interval's value to the passed interval. This is backwards to what I would expect, but
   * this makes it match the other existing add methods.
   *
   * @param interval intval to add
   */
  public void add(RedshiftIntervalDayToSecond interval) {
    interval.setValue(0, totalMicroseconds() + interval.totalMicroseconds());
  }

  /**
   * Returns whether an object is equal to this one or not.
   *
   * @param obj Object to compare with
   * @return true if the two intervals are identical
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof RedshiftIntervalDayToSecond)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    final RedshiftIntervalDayToSecond pgi = (RedshiftIntervalDayToSecond) obj;

    return pgi.getDays() == this.getDays() &&
           pgi.getHours() == this.getHours() &&
           pgi.getMinutes() == this.getMinutes() &&
           pgi.getWholeSeconds() == this.getWholeSeconds() &&
           pgi.getMicroSeconds() == this.getMicroSeconds();
  }
}
