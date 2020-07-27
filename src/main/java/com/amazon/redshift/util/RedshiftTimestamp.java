/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This class augments the Java built-in Timestamp to allow for explicit setting of the time zone.
 */
public class RedshiftTimestamp extends Timestamp {
  /**
   * The serial version UID.
   */
  private static final long serialVersionUID = -6245623465210738466L;

  /**
   * The special keyword for -infinity values.
   */
  private static final String MINUS_INFINITY_KEYWORD = "-infinity";
  
  /**
   * The special keyword for infinity values.
   */
  private static final String INFINITY_KEYWORD = "infinity";
  
  /**
   * Threadsafe access to ready to use date formatter
   */
  private static ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
      new ThreadLocal<SimpleDateFormat>()
      {
          @Override
          protected SimpleDateFormat initialValue()
          {
              return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          }
      };
  
  /**
   * Constant to represent a timezone that is invalid
   */
  private static final int UNINITIALIZED_TIMEZONE = 25;
  
  /**
   * The first time in millis in AD ('0001-01-01 00:00:00 AD')
   */
  private static final long FIRST_AD_TIMESTAMP = -62135769599767L;
  
      
  /**
   * The optional calendar for this timestamp.
   */
  private Calendar calendar;
  
  private String s;
  
  private int offSetHour;
  
  private int offSetMinute;
  
  /**
   * True if the timestamp is a positive value false otherwise
   */
  private boolean isAD = true;

  /**
   * Constructs a <code>RedshiftTimestamp</code> without a time zone. The integral seconds are stored in
   * the underlying date value; the fractional seconds are stored in the <code>nanos</code> field of
   * the <code>Timestamp</code> object.
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT. A negative number is the number
   *        of milliseconds before January 1, 1970, 00:00:00 GMT.
   * @see Timestamp#Timestamp(long)
   */
  public RedshiftTimestamp(long time) {
    this(time, null);
  }

  /**
   * <p>Constructs a <code>RedshiftTimestamp</code> with the given time zone. The integral seconds are stored
   * in the underlying date value; the fractional seconds are stored in the <code>nanos</code> field
   * of the <code>Timestamp</code> object.</p>
   *
   * <p>The calendar object is optional. If absent, the driver will treat the timestamp as
   * <code>timestamp without time zone</code>. When present, the driver will treat the timestamp as
   * a <code>timestamp with time zone</code> using the <code>TimeZone</code> in the calendar object.
   * Furthermore, this calendar will be used instead of the calendar object passed to
   * {@link java.sql.PreparedStatement#setTimestamp(int, Timestamp, Calendar)}.</p>
   *
   * @param time milliseconds since January 1, 1970, 00:00:00 GMT. A negative number is the number
   *        of milliseconds before January 1, 1970, 00:00:00 GMT.
   * @param calendar the calendar object containing the time zone or <code>null</code>.
   * @see Timestamp#Timestamp(long)
   */
  public RedshiftTimestamp(long time, Calendar calendar) {
  	this(time, calendar, null);
  }

  public RedshiftTimestamp(long time, Calendar calendar, 
  									 				String s) {
    super(time);
    this.setCalendar(calendar);
    this.s = s;
    if(calendar != null) {
      int rawOffset = calendar.getTimeZone().getRawOffset();
      this.offSetHour = rawOffset / 3600000;
      this.offSetMinute = (rawOffset / 60000) % 60;
    }
    
    // Determine the ERA (BC or AD)
    if (FIRST_AD_TIMESTAMP > time)
    {
        isAD = false;
    }
  }
  
  /**
   * Sets the calendar object for this timestamp.
   *
   * @param calendar the calendar object or <code>null</code>.
   */
  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  /**
   * Returns the calendar object for this timestamp.
   *
   * @return the calendar object or <code>null</code>.
   */
  public Calendar getCalendar() {
    return calendar;
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
    if (!(obj instanceof RedshiftTimestamp)) {
      return false;
    }
    RedshiftTimestamp other = (RedshiftTimestamp) obj;
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
    RedshiftTimestamp clone = (RedshiftTimestamp) super.clone();
    if (getCalendar() != null) {
      clone.setCalendar((Calendar) getCalendar().clone());
    }
    return clone;
  }
  
  @Override
  public String toString() {
  	if (s == null)
  		return super.toString();
  	else {
  		String s2 = getRedshiftString();
//  		System.out.println(" s=" + s + " s2=" + s2);
  		return s2;
  	}
  }
  
  /**
   * Convert the given time from the JVM local to an 'equivalent' time that is in the timezone of
   * the given calendar.
   * <p>
   * 'Equivalent' in this case means that the Day/Month/Year/Hour/Minute/Second/Millisecond
   * fields are equal if you interpret the input long as being in the 'from' calendar's timezone
   * and you interpret the output long as being in the 'to' calendar's timezone.
   *
   * @param timeMillis                The time to convert.
   * @param to                        The timezone to convert to.
   * @param from                      The timezone of the original data.
   *
   * @return the given time in the local time zone.
   */
  private static long convertTimeMillis(long timeMillis, Calendar to, Calendar from)
  {
      from.setTimeInMillis(timeMillis);

      to.set(from.get(Calendar.YEAR),
          from.get(Calendar.MONTH),
          from.get(Calendar.DAY_OF_MONTH),
          from.get(Calendar.HOUR_OF_DAY),
          from.get(Calendar.MINUTE),
          from.get(Calendar.SECOND));
      to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
      to.set(Calendar.ERA, from.get(Calendar.ERA));

      return to.getTimeInMillis();
  }

  /**
   * Get the Timestamp object adjusted to the JVM timezone.
   *
   * @return The adjusted Timestamp object.
   */
  private synchronized Timestamp getAdjustedTimestamp()
  {
      return getTimestamp(this, Calendar.getInstance(), calendar);
  }
  
  /**
   * Returns a Timestamp object set to the timezone of the given calendar. The Timestamp input
   * must be created with the default JVM timezone.
   *
   * @param timestamp                The Timestamp object to set.
   * @param to                       The java.util.Calendar object to use in constructing the
   *                                 converted Date. Cannot be null.
   * @param from                     The Calendar object used to create the input Date.
   *                                 Cannot be null.
   *
   * @return The new Timestamp object, or null if a null Timestamp was passed in.

   */
  private static Timestamp getTimestamp(Timestamp timestamp, Calendar to, Calendar from)
  {
      if (null == timestamp)
      {
          return null;
      }

      if (null == to || null == from)
      {
          throw new NullPointerException("Calendar cannot be null.");
      }

      to.clear();
      from.clear();
      if (to.equals(from))
      {
          // No need to convert.
          return timestamp;
      }

      // Retrieve millisecond time
      Timestamp tz = new Timestamp(convertTimeMillis(timestamp.getTime(), to, from));
      tz.setNanos(timestamp.getNanos());

      return tz;
  }
  
  /**
   * Specialized to string method to return the timestamp in a format that is the same as
   * opensource driver
   *
   * @return String to match the opensource driver's get string method
   */
  public String getPostgresqlString() {
  	return getRedshiftString();
  }
  
  private String getRedshiftString()
  {
      //Handle the negative and positive infinity case
      if (this.getTime() == Long.MIN_VALUE)
      {
          return MINUS_INFINITY_KEYWORD;
      }
      else if (this.getTime() == Long.MAX_VALUE)
      {
          return INFINITY_KEYWORD;
      }
      else
      {
          StringBuilder baseResult = new StringBuilder();

          Calendar cal = this.getCalendar();
          cal.setTime(this.getAdjustedTimestamp());

          baseResult.append(TIMESTAMP_FORMAT.get().format(cal.getTime()));

          String nanoSeconds = String.valueOf(this.getNanos());
          if (0 < this.getNanos())
          {
              baseResult.append(".");
              baseResult.append(nanoSeconds);

              // TIMESTAMP columns store values with up to a maximum of 6 digits of precision for fractional seconds.
              // If TIMESTAMP has more than 6 digits, trim the last 3 digit at the end of TIMESTAMP.
              if (6 < nanoSeconds.length())
              {
                  baseResult.delete((baseResult.length() - (nanoSeconds.length() - 6)), baseResult.length());
              }
          }

          if (UNINITIALIZED_TIMEZONE != offSetHour)
          {
              baseResult.append(offSetHour < 0 ? '-' : '+');
              if (0 != offSetHour)
              {
                  baseResult.append(String.format("%02d", Math.abs(offSetHour)));
              }
              else
              {
                  baseResult.append("00");
              }

              if (0 != offSetMinute)
              {
                  baseResult.append(":");
                  baseResult.append(String.format("%02d", Math.abs(offSetMinute)));
              }
          }

          if (!isAD)
          {
              baseResult.append(" BC");
          }

          return baseResult.toString();
      }
  }
}
