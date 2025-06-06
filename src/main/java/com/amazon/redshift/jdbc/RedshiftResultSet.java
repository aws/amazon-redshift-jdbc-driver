/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.RedshiftResultSetMetaData;
import com.amazon.redshift.RedshiftStatement;
import com.amazon.redshift.core.BaseConnection;
import com.amazon.redshift.core.BaseStatement;
import com.amazon.redshift.core.Encoding;
import com.amazon.redshift.core.Field;
import com.amazon.redshift.core.Oid;
import com.amazon.redshift.core.Query;
import com.amazon.redshift.core.ResultCursor;
import com.amazon.redshift.core.ResultHandlerBase;
import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.core.TypeInfo;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.core.v3.MessageLoopState;
import com.amazon.redshift.core.v3.RedshiftRowsBlockingQueue;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.ByteConverter;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.HStoreConverter;
import com.amazon.redshift.util.RedshiftBytea;
import com.amazon.redshift.util.RedshiftObject;
import com.amazon.redshift.util.RedshiftTokenizer;
import com.amazon.redshift.util.RedshiftVarbyte;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftGeography;
import com.amazon.redshift.util.RedshiftGeometry;
import com.amazon.redshift.util.RedshiftIntervalYearToMonth;
import com.amazon.redshift.util.RedshiftIntervalDayToSecond;
import com.amazon.redshift.util.RedshiftState;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
//JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
//JCP! endif
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

public class RedshiftResultSet implements ResultSet, com.amazon.redshift.RedshiftRefCursorResultSet {

  // needed for updateable result set support
  private boolean updateable = false;
  private boolean doingUpdates = false;
  private HashMap<String, Object> updateValues = null;
  private boolean usingOID = false; // are we using the OID for the primary key?
  private List<PrimaryKey> primaryKeys; // list of primary keys
  private boolean singleTable = false;
  private String onlyTable = "";
  private String tableName = null;
  private PreparedStatement updateStatement = null;
  private PreparedStatement insertStatement = null;
  private PreparedStatement deleteStatement = null;
  private PreparedStatement selectStatement = null;
  private final int resultsettype;
  private final int resultsetconcurrency;
  private int fetchdirection = ResultSet.FETCH_UNKNOWN;
  private TimeZone defaultTimeZone;
  protected final BaseConnection connection; // the connection we belong to
  protected final BaseStatement statement; // the statement we belong to
  protected final Field[] fields; // Field metadata for this resultset.
  protected final Query originalQuery; // Query we originated from

  protected final int maxRows; // Maximum rows in this resultset (might be 0).
  protected final int maxFieldSize; // Maximum field size in this resultset (might be 0).

  protected List<Tuple> rows; // Current page of results.
  protected RedshiftRowsBlockingQueue<Tuple> queueRows; // Results in a blocking queue.
  protected int[] rowCount;
  protected Thread ringBufferThread;
  protected int currentRow = -1; // Index into 'rows' of our currrent row (0-based)
  protected int rowOffset; // Offset of row 0 in the actual resultset
  protected Tuple thisRow; // copy of the current result row
  protected RedshiftWarningWrapper warningChain; // The warning chain
  /**
   * True if the last obtained column value was SQL NULL as specified by {@link #wasNull}. The value
   * is always updated by the {@link #checkResultSet} method.
   */
  protected boolean wasNullFlag = false;
  protected boolean onInsertRow = false;
  // are we on the insert row (for JDBC2 updatable resultsets)?

  private Tuple rowBuffer = null; // updateable rowbuffer

  protected int fetchSize; // Current fetch size (might be 0).
  protected ResultCursor cursor; // Cursor for fetching additional data.

  private Map<String, Integer> columnNameIndexMap; // Speed up findColumn by caching lookups

  private ResultSetMetaData rsMetaData;

  protected ResultSetMetaData createMetaData() throws SQLException {
    return new RedshiftResultSetMetaDataImpl(connection, fields);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    checkClosed();
    if (rsMetaData == null) {
      rsMetaData = createMetaData();
    }
    return rsMetaData;
  }

  RedshiftResultSet(Query originalQuery, BaseStatement statement, Field[] fields, List<Tuple> tuples,
      ResultCursor cursor, int maxRows, int maxFieldSize, int rsType, int rsConcurrency,
      int rsHoldability, RedshiftRowsBlockingQueue<Tuple> queueTuples,
      int[] rowCount, Thread ringBufferThread) throws SQLException {
    // Fail-fast on invalid null inputs
    if (tuples == null && queueTuples == null) {
      throw new NullPointerException("tuples or queueTuples must be non-null");
    }
    if (fields == null) {
      throw new NullPointerException("fields must be non-null");
    }

    this.originalQuery = originalQuery;
    this.connection = (BaseConnection) statement.getConnection();
    this.statement = statement;
    this.fields = fields;
    this.rows = tuples;
    this.cursor = cursor;
    this.maxRows = maxRows;
    this.maxFieldSize = maxFieldSize;
    this.resultsettype = rsType;
    this.resultsetconcurrency = rsConcurrency;
    this.queueRows = queueTuples;
    this.rowCount = rowCount;
    this.ringBufferThread = ringBufferThread; 
  }

  /**
   * Returns the number of rows in the result set. The value returned is undefined if the row
   * count is unknown.
   * <p>
   * The value returned must fit into a 32-bit integer when targeting a 32-bit platform, or a
   * 64-bit integer for a 64-bit platform. The value must be non-negative, except if the row count
   * is unknown.
   * <p>
   *
   * This is not a JDBC specification method.
   * This is for backward compatibility only.
   * 
   * @return Number of rows in the result set.
   * @throws SQLException
   *             If an error occurs.
   */
  public long getRowCount() throws SQLException {
    checkClosed();
  	return (rowCount != null) ? rowCount[0] : -1;
  }
  
  public java.net.URL getURL(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getURL columnIndex: {0}", columnIndex);
    checkClosed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getURL(int)");
  }

  public java.net.URL getURL(String columnName) throws SQLException {
    return getURL(findColumn(columnName));
  }

  protected Object internalGetObject(int columnIndex, Field field) throws SQLException {
    switch (getSQLType(columnIndex)) {
      case Types.BOOLEAN:
      case Types.BIT:
        return getBoolean(columnIndex);
      case Types.SQLXML:
        return getSQLXML(columnIndex);
      case Types.SMALLINT:
        return getShort(columnIndex);
      case Types.TINYINT:
        return getByte(columnIndex);
      case Types.INTEGER:
        return getInt(columnIndex);
      case Types.BIGINT:
        return getLong(columnIndex);
      case Types.NUMERIC:
      case Types.DECIMAL:
        return getNumeric(columnIndex,
            (field.getMod() == -1) ? -1 : ((field.getMod() - 4) & 0xffff), true);
      case Types.REAL:
        return getFloat(columnIndex);
      case Types.FLOAT:
      case Types.DOUBLE:
        return getDouble(columnIndex);
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return getString(columnIndex);
      case Types.DATE:
        return getDate(columnIndex);
      case Types.TIME:
      //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
      case Types.TIME_WITH_TIMEZONE:
      //JCP! endif
        return getTime(columnIndex);
      case Types.TIMESTAMP:
      //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
      case Types.TIMESTAMP_WITH_TIMEZONE:
      //JCP! endif
        return getTimestamp(columnIndex, null);
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return getBytes(columnIndex);
      case Types.ARRAY:
        return getArray(columnIndex);
      case Types.CLOB:
        return getClob(columnIndex);
      case Types.BLOB:
        return getBlob(columnIndex);
      case Types.OTHER:
        if (field.getOID() == Oid.INTERVALY2M)
          return getIntervalYearToMonth(columnIndex);
        if (field.getOID() == Oid.INTERVALD2S)
          return getIntervalDayToSecond(columnIndex);

      default:
        String type = getRSType(columnIndex);

        // if the backend doesn't know the type then coerce to String
        if (type.equals("unknown")) {
          return getString(columnIndex);
        }

        if (type.equals("uuid")) {
          if (isBinary(columnIndex)) {
            return getUUID(thisRow.get(columnIndex - 1));
          }
          return getUUID(getString(columnIndex));
        }

        // Specialized support for ref cursors is neater.
        if (type.equals("refcursor")) {
          // Fetch all results.
          String cursorName = getString(columnIndex);

          StringBuilder sb = new StringBuilder("FETCH ALL IN ");
          Utils.escapeIdentifier(sb, cursorName);

          // nb: no BEGIN triggered here. This is fine. If someone
          // committed, and the cursor was not holdable (closing the
          // cursor), we avoid starting a new xact and promptly causing
          // it to fail. If the cursor *was* holdable, we don't want a
          // new xact anyway since holdable cursor state isn't affected
          // by xact boundaries. If our caller didn't commit at all, or
          // autocommit was on, then we wouldn't issue a BEGIN anyway.
          //
          // We take the scrollability from the statement, but until
          // we have updatable cursors it must be readonly.
          ResultSet rs =
              connection.execSQLQuery(sb.toString(), resultsettype, ResultSet.CONCUR_READ_ONLY);
          //
          // In long running transactions these backend cursors take up memory space
          // we could close in rs.close(), but if the transaction is closed before the result set,
          // then
          // the cursor no longer exists

          sb.setLength(0);
          sb.append("CLOSE ");
          Utils.escapeIdentifier(sb, cursorName);
          connection.execSQLUpdate(sb.toString());
          ((RedshiftResultSet) rs).setRefCursor(cursorName);
          return rs;
        }
        if ("hstore".equals(type)) {
          if (isBinary(columnIndex)) {
            return HStoreConverter.fromBytes(thisRow.get(columnIndex - 1), connection.getEncoding());
          }
          return HStoreConverter.fromString(getString(columnIndex));
        }

        // Caller determines what to do (JDBC3 overrides in this case)
        return null;
    }
  }

  private void checkScrollable() throws SQLException {
    checkClosed();
    if (resultsettype == ResultSet.TYPE_FORWARD_ONLY) {
      throw new RedshiftException(
          GT.tr("Operation requires a scrollable ResultSet, but this ResultSet is FORWARD_ONLY."),
          RedshiftState.INVALID_CURSOR_STATE);
    }
  }

  @Override
  public boolean absolute(int index) throws SQLException {
    checkScrollable();

    // index is 1-based, but internally we use 0-based indices
    int internalIndex;

    if (index == 0) {
      beforeFirst();
      return false;
    }

    final int rows_size = rows.size();

    // if index<0, count from the end of the result set, but check
    // to be sure that it is not beyond the first index
    if (index < 0) {
      if (index >= -rows_size) {
        internalIndex = rows_size + index;
      } else {
        beforeFirst();
        return false;
      }
    } else {
      // must be the case that index>0,
      // find the correct place, assuming that
      // the index is not too large
      if (index <= rows_size) {
        internalIndex = index - 1;
      } else {
        afterLast();
        return false;
      }
    }

    currentRow = internalIndex;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }

  @Override
  public void afterLast() throws SQLException {
    checkScrollable();

    final int rows_size = rows.size();
    if (rows_size > 0) {
      currentRow = rows_size;
    }

    onInsertRow = false;
    thisRow = null;
    rowBuffer = null;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkScrollable();

    if (!rows.isEmpty()) {
      currentRow = -1;
    }

    onInsertRow = false;
    thisRow = null;
    rowBuffer = null;
  }

  @Override
  public boolean first() throws SQLException {
    checkScrollable();

    if (rows.size() <= 0) {
      return false;
    }

    currentRow = 0;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }

  @Override
  public Array getArray(String colName) throws SQLException {
    return getArray(findColumn(colName));
  }

  protected Array makeArray(int oid, byte[] value) throws SQLException {
    return new RedshiftArray(connection, oid, value);
  }

  protected Array makeArray(int oid, String value) throws SQLException {
    return new RedshiftArray(connection, oid, value);
  }

  @Override
  public Array getArray(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    int oid = fields[i - 1].getOID();
    if (isBinary(i)) {
      return makeArray(oid, thisRow.get(i - 1));
    }
    return makeArray(oid, getFixedString(i));
  }

  public java.math.BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getBigDecimal(columnIndex, -1);
  }

  public java.math.BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }

  public Blob getBlob(String columnName) throws SQLException {
    return getBlob(findColumn(columnName));
  }

  protected Blob makeBlob(long oid) throws SQLException {
    return new RedshiftBlob(connection, oid);
  }

  public Blob getBlob(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    return makeBlob(getLong(i));
  }

  public java.io.Reader getCharacterStream(String columnName) throws SQLException {
    return getCharacterStream(findColumn(columnName));
  }

  public java.io.Reader getCharacterStream(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    // Version 7.2 supports AsciiStream for all the RS text types
    // As the spec/javadoc for this method indicate this is to be used for
    // large text values (i.e. LONGVARCHAR) RS doesn't have a separate
    // long string datatype, but with toast the text datatype is capable of
    // handling very large values. Thus the implementation ends up calling
    // getString() since there is no current way to stream the value from the server
    return new CharArrayReader(getString(i).toCharArray());
  }

  public Clob getClob(String columnName) throws SQLException {
    return getClob(findColumn(columnName));
  }

  protected Clob makeClob(long oid) throws SQLException {
    return new RedshiftClob(connection, oid);
  }

  public Clob getClob(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    return makeClob(getLong(i));
  }

  public int getConcurrency() throws SQLException {
    checkClosed();
    return resultsetconcurrency;
  }

  @Override
  public java.sql.Date getDate(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    if (isBinary(i)) {
      int col = i - 1;
      int oid = fields[col].getOID();
      TimeZone tz = cal.getTimeZone();
      if (oid == Oid.DATE) {
        return connection.getTimestampUtils().toDateBin(tz, thisRow.get(col));
      } else if (oid == Oid.TIMESTAMP || oid == Oid.TIMESTAMPTZ) {
        // If backend provides just TIMESTAMP, we use "cal" timezone
        // If backend provides TIMESTAMPTZ, we ignore "cal" as we know true instant value
        Timestamp timestamp = getTimestamp(i, cal);
        // Here we just truncate date to 00:00 in a given time zone
        return connection.getTimestampUtils().convertToDate(timestamp.getTime(), tz);
      } else {
        	return connection.getTimestampUtils().toDate(cal, getString(i));
      }
    }

    return connection.getTimestampUtils().toDate(cal, getString(i));
  }

  @Override
  public Time getTime(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    if (isBinary(i)) {
      int col = i - 1;
      int oid = fields[col].getOID();
      TimeZone tz = cal.getTimeZone();
      if (oid == Oid.TIME || oid == Oid.TIMETZ) {
        return connection.getTimestampUtils().toTimeBin(tz, thisRow.get(col));
      } else if (oid == Oid.TIMESTAMP || oid == Oid.TIMESTAMPTZ) {
        // If backend provides just TIMESTAMP, we use "cal" timezone
        // If backend provides TIMESTAMPTZ, we ignore "cal" as we know true instant value
        Timestamp timestamp = getTimestamp(i, cal);
        long timeMillis = timestamp.getTime();
        return connection.getTimestampUtils().convertToTime(timeMillis, tz, timestamp.getNanos());
      } else {
        throw new RedshiftException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), "time"),
            RedshiftState.DATA_TYPE_MISMATCH);
      }
    }

    String string = getString(i);
    return connection.getTimestampUtils().toTime(cal, string);
  }

  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
  private LocalTime getLocalTime(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(i)) {
      int col = i - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.TIME) {
        return connection.getTimestampUtils().toLocalTimeBin(thisRow.get(col));
      } else {
        throw new RedshiftException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), "time"),
            RedshiftState.DATA_TYPE_MISMATCH);
      }
    }

    String string = getString(i);
    return connection.getTimestampUtils().toLocalTime(string);
  }
  //JCP! endif

  @Override
  public Timestamp getTimestamp(int i, java.util.Calendar cal) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    if (cal == null) {
      cal = getDefaultCalendar();
    }
    int col = i - 1;
    int oid = fields[col].getOID();
    if (isBinary(i)) {
      if (oid == Oid.TIMESTAMPTZ || oid == Oid.TIMESTAMP || oid == Oid.ABSTIMEOID) {
        boolean hasTimeZone = oid == Oid.TIMESTAMPTZ;
        TimeZone tz = cal.getTimeZone();
        if(oid == Oid.ABSTIMEOID)
          return connection.getTimestampUtils().toTimestampAbsTimeBin(tz, thisRow.get(col), hasTimeZone, cal);
        else
          return connection.getTimestampUtils().toTimestampBin(tz, thisRow.get(col), hasTimeZone, cal);
      } else {
        // JDBC spec says getTimestamp of Time and Date must be supported
        long millis;
        if (oid == Oid.TIME || oid == Oid.TIMETZ) {
          boolean hasTimeZone = oid == Oid.TIMETZ;
          return connection.getTimestampUtils().convertToTimestamp(getTime(i, cal).getTime(), hasTimeZone, connection.getTimestampUtils().getNanos(thisRow.get(col)), cal);
        } else if (oid == Oid.DATE) {
          millis = getDate(i, cal).getTime();
          return new Timestamp(millis);
        } 
      }
    }

    // If this is actually a timestamptz, the server-provided timezone will override
    // the one we pass in, which is the desired behaviour. Otherwise, we'll
    // interpret the timezone-less value in the provided timezone.
    String string = getString(i);
    if (oid == Oid.TIME || oid == Oid.TIMETZ) {
      // If server sends us a TIME, we ensure java counterpart has date of 1970-01-01
      boolean hasTimeZone = oid == Oid.TIMETZ;
      return connection.getTimestampUtils().convertToTimestamp(connection.getTimestampUtils().toTime(cal, string).getTime(), hasTimeZone, connection.getTimestampUtils().getNanos(string), cal);
    }
    return connection.getTimestampUtils().toTimestamp(cal, string);
  }

  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
  private OffsetDateTime getOffsetDateTime(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    int col = i - 1;
    int oid = fields[col].getOID();

    if (isBinary(i)) {
      if (oid == Oid.TIMESTAMPTZ || oid == Oid.TIMESTAMP) {
        return connection.getTimestampUtils().toOffsetDateTimeBin(thisRow.get(col));
      } else if (oid == Oid.TIMETZ) {
        // JDBC spec says timetz must be supported
        Time time = getTime(i);
        return connection.getTimestampUtils().toOffsetDateTime(time);
      } else {
        throw new RedshiftException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), "timestamptz"),
            RedshiftState.DATA_TYPE_MISMATCH);
      }
    }

    // If this is actually a timestamptz, the server-provided timezone will override
    // the one we pass in, which is the desired behaviour. Otherwise, we'll
    // interpret the timezone-less value in the provided timezone.
    String string = getString(i);
    if (oid == Oid.TIMETZ) {
      // JDBC spec says timetz must be supported
      // If server sends us a TIMETZ, we ensure java counterpart has date of 1970-01-01
      Calendar cal = getDefaultCalendar();
      Time time = connection.getTimestampUtils().toTime(cal, string);
      return connection.getTimestampUtils().toOffsetDateTime(time);
    }
    return connection.getTimestampUtils().toOffsetDateTime(string);
  }

  private LocalDateTime getLocalDateTime(int i) throws SQLException {
    checkResultSet(i);
    if (wasNullFlag) {
      return null;
    }

    int col = i - 1;
    int oid = fields[col].getOID();
    if (oid != Oid.TIMESTAMP) {
      throw new RedshiftException(
              GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                  Oid.toString(oid), "timestamp"),
              RedshiftState.DATA_TYPE_MISMATCH);
    }
    if (isBinary(i)) {
      return connection.getTimestampUtils().toLocalDateTimeBin(thisRow.get(col));
    }

    String string = getString(i);
    return connection.getTimestampUtils().toLocalDateTime(string);
  }
  //JCP! endif

  public java.sql.Date getDate(String c, java.util.Calendar cal) throws SQLException {
    return getDate(findColumn(c), cal);
  }

  public Time getTime(String c, java.util.Calendar cal) throws SQLException {
    return getTime(findColumn(c), cal);
  }

  public Timestamp getTimestamp(String c, java.util.Calendar cal) throws SQLException {
    return getTimestamp(findColumn(c), cal);
  }

  public int getFetchDirection() throws SQLException {
    checkClosed();
    return fetchdirection;
  }

  public Object getObjectImpl(String columnName, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(findColumn(columnName), map);
  }

  /*
   * This checks against map for the type of column i, and if found returns an object based on that
   * mapping. The class must implement the SQLData interface.
   */
  public Object getObjectImpl(int i, Map<String, Class<?>> map) throws SQLException {
    checkClosed();
    if (map == null || map.isEmpty()) {
      return getObject(i);
    }
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getObjectImpl(int,Map)");
  }

  public Ref getRef(String columnName) throws SQLException {
    return getRef(findColumn(columnName));
  }

  public Ref getRef(int i) throws SQLException {
    checkClosed();
    // The backend doesn't yet have SQL3 REF types
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getRef(int)");
  }

  @Override
  public int getRow() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      return 0;
    }

    if (queueRows != null) {
    	int rowIndex = queueRows.getCurrentRowIndex();
    	
    	if (rowIndex < 0 || rowIndex >= getRowCount())
    		return 0;
    	else
    		return rowIndex + 1;
    }
    else {
	    final int rows_size = rows.size();
	
	    if (currentRow < 0 || currentRow >= rows_size) {
	      return 0;
	    }
	
	    return rowOffset + currentRow + 1;
    }
  }

  // This one needs some thought, as not all ResultSets come from a statement
  public Statement getStatement() throws SQLException {
    checkClosed();
    return statement;
  }

  public int getType() throws SQLException {
    checkClosed();
    return resultsettype;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    if (queueRows != null) {
    	if(getRowCount() == 0)
    		return false;
    	
    	return(queueRows.endOfResult()
    					&& queueRows.getCurrentRowIndex() >= getRowCount());
    }
    else {
	    final int rows_size = rows.size();
	    if (rowOffset + rows_size == 0) {
	      return false;
	    }
	    return (currentRow >= rows_size);
    }
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    if (queueRows != null) {
    	return (queueRows.getCurrentRowIndex() < 0 
    					&& getRowCount() > 0);
    }
    else {
    	return ((rowOffset + currentRow) < 0 && !rows.isEmpty());
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    if (queueRows != null) {
    	if(getRowCount() == 0)
    		return false;
    	
    	return(queueRows.getCurrentRowIndex() == 0);
    }
    else {
	    final int rows_size = rows.size();
	    if (rowOffset + rows_size == 0) {
	      return false;
	    }
	
	    return ((rowOffset + currentRow) == 0);
    }
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      return false;
    }

    if (queueRows != null) {
    	if(getRowCount() == 0)
    		return false;
    	
    	return(queueRows.endOfResult()
    					&& ((queueRows.getCurrentRowIndex() + 1) == getRowCount()));
    }
    else {
	    final int rows_size = rows.size();
	
	    if (rows_size == 0) {
	      return false; // No rows.
	    }
	
	    if (currentRow != (rows_size - 1)) {
	      return false; // Not on the last row of this block.
	    }
	
	    // We are on the last row of the current block.
	
	    if (cursor == null) {
	      // This is the last block and therefore the last row.
	      return true;
	    }
	
	    if (maxRows > 0 && rowOffset + currentRow == maxRows) {
	      // We are implicitly limited by maxRows.
	      return true;
	    }
	
	    // Now the more painful case begins.
	    // We are on the last row of the current block, but we don't know if the
	    // current block is the last block; we must try to fetch some more data to
	    // find out.
	
	    // We do a fetch of the next block, then prepend the current row to that
	    // block (so currentRow == 0). This works as the current row
	    // must be the last row of the current block if we got this far.
	
	    rowOffset += rows_size - 1; // Discarding all but one row.
	
	    // Work out how many rows maxRows will let us fetch.
	    int fetchRows = fetchSize;
	    if (maxRows != 0) {
	      if (fetchRows == 0 || rowOffset + fetchRows > maxRows) {
	        // Fetch would exceed maxRows, limit it.
	        fetchRows = maxRows - rowOffset;
	      }
	    }
	
      Properties inProps = ((RedshiftConnectionImpl) connection).getConnectionProperties();
	    // Do the actual fetch.
	    connection.getQueryExecutor().fetch(cursor, new CursorResultHandler(inProps), fetchRows, 0);
	
	    // Now prepend our one saved row and move to it.
	    rows.add(0, thisRow);
	    currentRow = 0;
	
	    // Finally, now we can tell if we're the last row or not.
	    return (rows.size() == 1);
    }
  }

  @Override
  public boolean last() throws SQLException {
    checkScrollable();

    final int rows_size = rows.size();
    if (rows_size <= 0) {
      return false;
    }

    currentRow = rows_size - 1;
    initRowBuffer();
    onInsertRow = false;

    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    checkScrollable();

    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Can''t use relative move methods while on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (currentRow - 1 < 0) {
      currentRow = -1;
      thisRow = null;
      rowBuffer = null;
      return false;
    } else {
      currentRow--;
    }
    initRowBuffer();
    return true;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkScrollable();

    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Can''t use relative move methods while on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    // have to add 1 since absolute expects a 1-based index
    int index = currentRow + 1 + rows;
    if (index < 0) {
      beforeFirst();
      return false;
    }
    return absolute(index);
  }

  public void setFetchDirection(int direction) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, direction);
  	
    checkClosed();
    switch (direction) {
      case ResultSet.FETCH_FORWARD:
        break;
      case ResultSet.FETCH_REVERSE:
      case ResultSet.FETCH_UNKNOWN:
        checkScrollable();
        break;
      default:
        throw new RedshiftException(GT.tr("Invalid fetch direction constant: {0}.", direction),
            RedshiftState.INVALID_PARAMETER_VALUE);
    }

    this.fetchdirection = direction;
  }

  public synchronized void cancelRowUpdates() throws SQLException {
    checkClosed();
    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Cannot call cancelRowUpdates() when on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (doingUpdates) {
      doingUpdates = false;

      clearRowBuffer(true);
    }
  }

  public synchronized void deleteRow() throws SQLException {
    checkUpdateable();

    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Cannot call deleteRow() when on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (isBeforeFirst()) {
      throw new RedshiftException(
          GT.tr(
              "Currently positioned before the start of the ResultSet.  You cannot call deleteRow() here."),
          RedshiftState.INVALID_CURSOR_STATE);
    }
    if (isAfterLast()) {
      throw new RedshiftException(
          GT.tr(
              "Currently positioned after the end of the ResultSet.  You cannot call deleteRow() here."),
          RedshiftState.INVALID_CURSOR_STATE);
    }
    
    if(queueRows != null) {
      throw new RedshiftException(GT.tr("Cannot call deleteRow() when enableFetchRingBuffer is true."),
          RedshiftState.INVALID_CURSOR_STATE);
    }
    else {
	    if (rows.isEmpty()) {
	      throw new RedshiftException(GT.tr("There are no rows in this ResultSet."),
	          RedshiftState.INVALID_CURSOR_STATE);
	    }
	
	    int numKeys = primaryKeys.size();
	    if (deleteStatement == null) {
	      StringBuilder deleteSQL =
	          new StringBuilder("DELETE FROM ").append(onlyTable).append(tableName).append(" where ");
	
	      for (int i = 0; i < numKeys; i++) {
	        Utils.escapeIdentifier(deleteSQL, primaryKeys.get(i).name);
	        deleteSQL.append(" = ?");
	        if (i < numKeys - 1) {
	          deleteSQL.append(" and ");
	        }
	      }
	
	      deleteStatement = connection.prepareStatement(deleteSQL.toString());
	    }
	    deleteStatement.clearParameters();
	
	    for (int i = 0; i < numKeys; i++) {
	      deleteStatement.setObject(i + 1, primaryKeys.get(i).getValue());
	    }
	
	    deleteStatement.executeUpdate();
	
	    rows.remove(currentRow);
	    currentRow--;
	    moveToCurrentRow();
    }
  }

  @Override
  public synchronized void insertRow() throws SQLException {
    checkUpdateable();

    if (!onInsertRow) {
      throw new RedshiftException(GT.tr("Not on the insert row."), RedshiftState.INVALID_CURSOR_STATE);
    } else if (updateValues.isEmpty()) {
      throw new RedshiftException(GT.tr("You must specify at least one column value to insert a row."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    } else if(queueRows != null) {
	      throw new RedshiftException(GT.tr("Cannot call insertRow() when enableFetchRingBuffer is true."),
	          RedshiftState.INVALID_CURSOR_STATE);
    } else {

      // loop through the keys in the insertTable and create the sql statement
      // we have to create the sql every time since the user could insert different
      // columns each time

      StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
      StringBuilder paramSQL = new StringBuilder(") values (");

      Iterator<String> columnNames = updateValues.keySet().iterator();
      int numColumns = updateValues.size();

      for (int i = 0; columnNames.hasNext(); i++) {
        String columnName = columnNames.next();

        Utils.escapeIdentifier(insertSQL, columnName);
        if (i < numColumns - 1) {
          insertSQL.append(", ");
          paramSQL.append("?,");
        } else {
          paramSQL.append("?)");
        }

      }

      insertSQL.append(paramSQL.toString());
      insertStatement = connection.prepareStatement(insertSQL.toString());

      Iterator<Object> values = updateValues.values().iterator();

      for (int i = 1; values.hasNext(); i++) {
        insertStatement.setObject(i, values.next());
      }

      insertStatement.executeUpdate();

      if (usingOID) {
        // we have to get the last inserted OID and put it in the resultset

        long insertedOID = ((RedshiftStatementImpl) insertStatement).getLastOID();

        updateValues.put("oid", insertedOID);

      }

      // update the underlying row to the new inserted data
      updateRowBuffer();

      rows.add(rowBuffer);

      // we should now reflect the current data in thisRow
      // that way getXXX will get the newly inserted data
      thisRow = rowBuffer;

      // need to clear this in case of another insert
      clearRowBuffer(false);
    }
  }

  @Override
  public synchronized void moveToCurrentRow() throws SQLException {
    checkUpdateable();

    if (currentRow < 0 || currentRow >= rows.size()) {
      thisRow = null;
      rowBuffer = null;
    } else {
      initRowBuffer();
    }

    onInsertRow = false;
    doingUpdates = false;
  }

  @Override
  public synchronized void moveToInsertRow() throws SQLException {
    checkUpdateable();

    if (insertStatement != null) {
      insertStatement = null;
    }

    // make sure the underlying data is null
    clearRowBuffer(false);

    onInsertRow = true;
    doingUpdates = false;
  }

  // rowBuffer is the temporary storage for the row
  private synchronized void clearRowBuffer(boolean copyCurrentRow) throws SQLException {

    // inserts want an empty array while updates want a copy of the current row
    if (copyCurrentRow) {
      rowBuffer = thisRow.updateableCopy();
    } else {
      rowBuffer = new Tuple(fields.length);
    }

    // clear the updateValues hash map for the next set of updates
    updateValues.clear();
  }

  public boolean rowDeleted() throws SQLException {
    checkClosed();
    return false;
  }

  public boolean rowInserted() throws SQLException {
    checkClosed();
    return false;
  }

  public boolean rowUpdated() throws SQLException {
    checkClosed();
    return false;
  }

  public synchronized void updateAsciiStream(int columnIndex, java.io.InputStream x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    try {
      InputStreamReader reader = new InputStreamReader(x, "ASCII");
      char[] data = new char[length];
      int numRead = 0;
      while (true) {
        int n = reader.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
      updateString(columnIndex, new String(data, 0, numRead));
    } catch (UnsupportedEncodingException uee) {
      throw new RedshiftException(GT.tr("The JVM claims not to support the encoding: {0}", "ASCII"),
          RedshiftState.UNEXPECTED_ERROR, uee);
    } catch (IOException ie) {
      throw new RedshiftException(GT.tr("Provided InputStream failed."), null, ie);
    }
  }

  public synchronized void updateBigDecimal(int columnIndex, java.math.BigDecimal x)
      throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateBinaryStream(int columnIndex, java.io.InputStream x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    byte[] data = new byte[length];
    int numRead = 0;
    try {
      while (true) {
        int n = x.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
    } catch (IOException ie) {
      throw new RedshiftException(GT.tr("Provided InputStream failed."), null, ie);
    }

    if (numRead == length) {
      updateBytes(columnIndex, data);
    } else {
      // the stream contained less data than they said
      // perhaps this is an error?
      byte[] data2 = new byte[numRead];
      System.arraycopy(data, 0, data2, 0, numRead);
      updateBytes(columnIndex, data2);
    }
  }

  public synchronized void updateBoolean(int columnIndex, boolean x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateByte(int columnIndex, byte x) throws SQLException {
    updateValue(columnIndex, String.valueOf(x));
  }

  public synchronized void updateBytes(int columnIndex, byte[] x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateCharacterStream(int columnIndex, java.io.Reader x, int length)
      throws SQLException {
    if (x == null) {
      updateNull(columnIndex);
      return;
    }

    try {
      char[] data = new char[length];
      int numRead = 0;
      while (true) {
        int n = x.read(data, numRead, length - numRead);
        if (n == -1) {
          break;
        }

        numRead += n;

        if (numRead == length) {
          break;
        }
      }
      updateString(columnIndex, new String(data, 0, numRead));
    } catch (IOException ie) {
      throw new RedshiftException(GT.tr("Provided Reader failed."), null, ie);
    }
  }

  public synchronized void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateDouble(int columnIndex, double x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateFloat(int columnIndex, float x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateInt(int columnIndex, int x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateLong(int columnIndex, long x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateNull(int columnIndex) throws SQLException {
    checkColumnIndex(columnIndex);
    String columnTypeName = getRSType(columnIndex);
    updateValue(columnIndex, new NullObject(columnTypeName));
  }

  public synchronized void updateObject(int columnIndex, Object x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    this.updateObject(columnIndex, x);
  }

  @Override
  public void refreshRow() throws SQLException {
    checkUpdateable();
    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Can''t refresh the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    } else if(queueRows != null) {
	    	throw new RedshiftException(GT.tr("Can''t refresh when enableFetchRingBuffer is true."),
	        RedshiftState.INVALID_CURSOR_STATE);
  	}
    
    if (isBeforeFirst() || isAfterLast() || rows.isEmpty()) {
      return;
    }

    StringBuilder selectSQL = new StringBuilder("select ");

    ResultSetMetaData rsmd = getMetaData();
    RedshiftResultSetMetaData pgmd = (RedshiftResultSetMetaData) rsmd;
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      if (i > 1) {
        selectSQL.append(", ");
      }
      selectSQL.append(pgmd.getBaseColumnName(i));
    }
    selectSQL.append(" from ").append(onlyTable).append(tableName).append(" where ");

    int numKeys = primaryKeys.size();

    for (int i = 0; i < numKeys; i++) {

      PrimaryKey primaryKey = primaryKeys.get(i);
      selectSQL.append(primaryKey.name).append("= ?");

      if (i < numKeys - 1) {
        selectSQL.append(" and ");
      }
    }
    String sqlText = selectSQL.toString();
    if (RedshiftLogger.isEnable()) {
      connection.getLogger().log(LogLevel.DEBUG, "selecting {0}", sqlText);
    }
    // because updateable result sets do not yet support binary transfers we must request refresh
    // with updateable result set to get field data in correct format
    selectStatement = connection.prepareStatement(sqlText,
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

    for (int j = 0, i = 1; j < numKeys; j++, i++) {
      selectStatement.setObject(i, primaryKeys.get(j).getValue());
    }

    RedshiftResultSet rs = (RedshiftResultSet) selectStatement.executeQuery();

    if (rs.next()) {
      rowBuffer = rs.thisRow;
    }

    rows.set(currentRow, rowBuffer);
    thisRow = rowBuffer;

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "done updates");

    rs.close();
    selectStatement.close();
    selectStatement = null;
  }

  @Override
  public synchronized void updateRow() throws SQLException {
    checkUpdateable();

    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Cannot call updateRow() when on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    } else if(queueRows != null) {
		  	throw new RedshiftException(GT.tr("Cannot call updateRow() when enableFetchRingBuffer is true."),
		      RedshiftState.INVALID_CURSOR_STATE);
    }

    if (isBeforeFirst() || isAfterLast() || rows.isEmpty()) {
      throw new RedshiftException(
          GT.tr(
              "Cannot update the ResultSet because it is either before the start or after the end of the results."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (!doingUpdates) {
      return; // No work pending.
    }

    StringBuilder updateSQL = new StringBuilder("UPDATE " + onlyTable + tableName + " SET  ");

    int numColumns = updateValues.size();
    Iterator<String> columns = updateValues.keySet().iterator();

    for (int i = 0; columns.hasNext(); i++) {
      String column = columns.next();
      Utils.escapeIdentifier(updateSQL, column);
      updateSQL.append(" = ?");

      if (i < numColumns - 1) {
        updateSQL.append(", ");
      }
    }

    updateSQL.append(" WHERE ");

    int numKeys = primaryKeys.size();

    for (int i = 0; i < numKeys; i++) {
      PrimaryKey primaryKey = primaryKeys.get(i);
      Utils.escapeIdentifier(updateSQL, primaryKey.name);
      updateSQL.append(" = ?");

      if (i < numKeys - 1) {
        updateSQL.append(" and ");
      }
    }

    String sqlText = updateSQL.toString();
    if (RedshiftLogger.isEnable()) {
      connection.getLogger().log(LogLevel.DEBUG, "updating {0}", sqlText);
    }
    updateStatement = connection.prepareStatement(sqlText);

    int i = 0;
    Iterator<Object> iterator = updateValues.values().iterator();
    for (; iterator.hasNext(); i++) {
      Object o = iterator.next();
      updateStatement.setObject(i + 1, o);
    }

    for (int j = 0; j < numKeys; j++, i++) {
      updateStatement.setObject(i + 1, primaryKeys.get(j).getValue());
    }

    updateStatement.executeUpdate();
    updateStatement.close();
    updateStatement = null;

    updateRowBuffer();

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "copying data");
    thisRow = rowBuffer.readOnlyCopy();
    rows.set(currentRow, rowBuffer);

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "done updates");
    updateValues.clear();
    doingUpdates = false;
  }

  public synchronized void updateShort(int columnIndex, short x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateString(int columnIndex, String x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateTime(int columnIndex, Time x) throws SQLException {
    updateValue(columnIndex, x);
  }

  public synchronized void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    updateValue(columnIndex, x);

  }

  public synchronized void updateNull(String columnName) throws SQLException {
    updateNull(findColumn(columnName));
  }

  public synchronized void updateBoolean(String columnName, boolean x) throws SQLException {
    updateBoolean(findColumn(columnName), x);
  }

  public synchronized void updateByte(String columnName, byte x) throws SQLException {
    updateByte(findColumn(columnName), x);
  }

  public synchronized void updateShort(String columnName, short x) throws SQLException {
    updateShort(findColumn(columnName), x);
  }

  public synchronized void updateInt(String columnName, int x) throws SQLException {
    updateInt(findColumn(columnName), x);
  }

  public synchronized void updateLong(String columnName, long x) throws SQLException {
    updateLong(findColumn(columnName), x);
  }

  public synchronized void updateFloat(String columnName, float x) throws SQLException {
    updateFloat(findColumn(columnName), x);
  }

  public synchronized void updateDouble(String columnName, double x) throws SQLException {
    updateDouble(findColumn(columnName), x);
  }

  public synchronized void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
    updateBigDecimal(findColumn(columnName), x);
  }

  public synchronized void updateString(String columnName, String x) throws SQLException {
    updateString(findColumn(columnName), x);
  }

  public synchronized void updateBytes(String columnName, byte[] x) throws SQLException {
    updateBytes(findColumn(columnName), x);
  }

  public synchronized void updateDate(String columnName, java.sql.Date x) throws SQLException {
    updateDate(findColumn(columnName), x);
  }

  public synchronized void updateTime(String columnName, java.sql.Time x) throws SQLException {
    updateTime(findColumn(columnName), x);
  }

  public synchronized void updateTimestamp(String columnName, java.sql.Timestamp x)
      throws SQLException {
    updateTimestamp(findColumn(columnName), x);
  }

  public synchronized void updateAsciiStream(String columnName, java.io.InputStream x, int length)
      throws SQLException {
    updateAsciiStream(findColumn(columnName), x, length);
  }

  public synchronized void updateBinaryStream(String columnName, java.io.InputStream x, int length)
      throws SQLException {
    updateBinaryStream(findColumn(columnName), x, length);
  }

  public synchronized void updateCharacterStream(String columnName, java.io.Reader reader,
      int length) throws SQLException {
    updateCharacterStream(findColumn(columnName), reader, length);
  }

  public synchronized void updateObject(String columnName, Object x, int scale)
      throws SQLException {
    updateObject(findColumn(columnName), x);
  }

  public synchronized void updateObject(String columnName, Object x) throws SQLException {
    updateObject(findColumn(columnName), x);
  }

  /**
   * Is this ResultSet updateable?
   */

  boolean isUpdateable() throws SQLException {
    checkClosed();

    if (resultsetconcurrency == ResultSet.CONCUR_READ_ONLY) {
      throw new RedshiftException(
          GT.tr("ResultSets with concurrency CONCUR_READ_ONLY cannot be updated."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (updateable) {
      return true;
    }

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "checking if rs is updateable");

    parseQuery();

    if (!singleTable) {
      if (RedshiftLogger.isEnable()) 
      	connection.getLogger().log(LogLevel.DEBUG, "not a single table");
      return false;
    }

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "getting primary keys");

    //
    // Contains the primary key?
    //

    primaryKeys = new ArrayList<PrimaryKey>();

    // this is not strictly jdbc spec, but it will make things much faster if used
    // the user has to select oid, * from table and then we will just use oid
    // with oids has been removed in version 12
    // FIXME: with oids does not automatically create an index, should check for primary keys first

    usingOID = false;
    int oidIndex = findColumnIndex("oid"); // 0 if not present

    int i = 0;
    int numPKcolumns = 0;

    // if we find the oid then just use it

    // oidIndex will be >0 if the oid was in the select list
    if (oidIndex > 0) {
      i++;
      numPKcolumns++;
      primaryKeys.add(new PrimaryKey(oidIndex, "oid"));
      usingOID = true;
    } else {
      // otherwise go and get the primary keys and create a list of keys
      String[] s = quotelessTableName(tableName);
      String quotelessTableName = s[0];
      String quotelessSchemaName = s[1];
      java.sql.ResultSet rs = connection.getMetaData().getPrimaryKeys("",
          quotelessSchemaName, quotelessTableName);
      while (rs.next()) {
        numPKcolumns++;
        String columnName = rs.getString(4); // get the columnName
        int index = findColumnIndex(columnName);

        if (index > 0) {
          i++;
          primaryKeys.add(new PrimaryKey(index, columnName)); // get the primary key information
        }
      }

      rs.close();
    }

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "no of keys={0}", i);

    if (i < 1) {
      throw new RedshiftException(GT.tr("No primary key found for table {0}.", tableName),
          RedshiftState.DATA_ERROR);
    }

    updateable = (i == numPKcolumns);

    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "checking primary key {0}", updateable);

    return updateable;
  }

  /**
   * Cracks out the table name and schema (if it exists) from a fully qualified table name.
   *
   * @param fullname string that we are trying to crack. Test cases:
   *
   *        <pre>
   *
   *                 Table: table
   *                                 ()
   *
   *                 "Table": Table
   *                                 ()
   *
   *                 Schema.Table:
   *                                 table (schema)
   *
   *                                 "Schema"."Table": Table
   *                                                 (Schema)
   *
   *                                 "Schema"."Dot.Table": Dot.Table
   *                                                 (Schema)
   *
   *                                 Schema."Dot.Table": Dot.Table
   *                                                 (schema)
   *
   *        </pre>
   *
   * @return String array with element zero always being the tablename and element 1 the schema name
   *         which may be a zero length string.
   */
  public static String[] quotelessTableName(String fullname) {

    String[] parts = new String[]{null, ""};
    StringBuilder acc = new StringBuilder();
    boolean betweenQuotes = false;
    for (int i = 0; i < fullname.length(); i++) {
      char c = fullname.charAt(i);
      switch (c) {
        case '"':
          if ((i < fullname.length() - 1) && (fullname.charAt(i + 1) == '"')) {
            // two consecutive quotes - keep one
            i++;
            acc.append(c); // keep the quote
          } else { // Discard it
            betweenQuotes = !betweenQuotes;
          }
          break;
        case '.':
          if (betweenQuotes) { // Keep it
            acc.append(c);
          } else { // Have schema name
            parts[1] = acc.toString();
            acc = new StringBuilder();
          }
          break;
        default:
          acc.append((betweenQuotes) ? c : Character.toLowerCase(c));
          break;
      }
    }
    // Always put table in slot 0
    parts[0] = acc.toString();
    return parts;
  }

  private void parseQuery() {
    String sql = originalQuery.toString(null);
    StringTokenizer st = new StringTokenizer(sql, " \r\t\n");
    boolean tableFound = false;
    boolean tablesChecked = false;
    String name = "";

    singleTable = true;

    while (!tableFound && !tablesChecked && st.hasMoreTokens()) {
      name = st.nextToken();
      if ("from".equalsIgnoreCase(name)) {
        tableName = st.nextToken();
        if ("only".equalsIgnoreCase(tableName)) {
          tableName = st.nextToken();
          onlyTable = "ONLY ";
        }
        tableFound = true;
      }
    }
  }

  private void updateRowBuffer() throws SQLException {
    for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
      int columnIndex = findColumn(entry.getKey()) - 1;

      Object valueObject = entry.getValue();
      if (valueObject instanceof RedshiftObject) {
        String value = ((RedshiftObject) valueObject).getValue();
        rowBuffer.set(columnIndex, (value == null) ? null : connection.encodeString(value));
      } else {
        switch (getSQLType(columnIndex + 1)) {

          // boolean needs to be formatted as t or f instead of true or false
          case Types.BIT:
          case Types.BOOLEAN:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[1];
	            ByteConverter.bool(val, 0, ((Boolean) valueObject).booleanValue());
	            rowBuffer.set(columnIndex, val);
            }
            else {
              rowBuffer.set(columnIndex, connection
                  .encodeString(((Boolean) valueObject).booleanValue() ? "t" : "f"));
            }
            break;
            //
            // toString() isn't enough for date and time types; we must format it correctly
            // or we won't be able to re-parse it.
            //
          case Types.DATE:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[4];
	            TimeZone tz = null;
	            connection.getTimestampUtils().toBinDate(tz, val, (Date) valueObject);
	            rowBuffer.set(columnIndex, val);
            }
            else {
	            rowBuffer.set(columnIndex, connection
	                .encodeString(
	                    connection.getTimestampUtils().toString(
	                        getDefaultCalendar(), (Date) valueObject)));
            }
            break;

          case Types.TIME:
          //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
          case Types.TIME_WITH_TIMEZONE:
          //JCP! endif
            rowBuffer.set(columnIndex, connection
                .encodeString(
                    connection.getTimestampUtils().toString(
                        getDefaultCalendar(), (Time) valueObject)));
            break;

          case Types.TIMESTAMP:
          //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
          case Types.TIMESTAMP_WITH_TIMEZONE:
          //JCP! endif
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[8];
	            connection.getTimestampUtils().toBinTimestamp(null, val, (Timestamp) valueObject);
	            rowBuffer.set(columnIndex, val);
            }
            else {
	            rowBuffer.set(columnIndex, connection.encodeString(
	                connection.getTimestampUtils().toString(
	                    getDefaultCalendar(), (Timestamp) valueObject)));
            }
            break;

          case Types.NULL:
            // Should never happen?
            break;

          case Types.BINARY:
          case Types.LONGVARBINARY:
          case Types.VARBINARY:
            if (isBinary(columnIndex + 1)) {
              rowBuffer.set(columnIndex, (byte[]) valueObject);
            } else {
              try {
                rowBuffer.set(columnIndex,
                    RedshiftBytea.toRSString((byte[]) valueObject).getBytes("ISO-8859-1"));
              } catch (UnsupportedEncodingException e) {
                throw new RedshiftException(
                    GT.tr("The JVM claims not to support the encoding: {0}", "ISO-8859-1"),
                    RedshiftState.UNEXPECTED_ERROR, e);
              }
            }
            break;
            
          case Types.SMALLINT:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[2];
	            ByteConverter.int2(val, 0, Integer.valueOf(String.valueOf(valueObject)));
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }
            
          	break;
          	
          case Types.INTEGER:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[4];
	            ByteConverter.int4(val, 0, Integer.valueOf(String.valueOf(valueObject)));
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }
            
          	break;

          case Types.BIGINT:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
	            byte[] val = new byte[8];
	            ByteConverter.int8(val, 0, Long.valueOf(String.valueOf(valueObject)));
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }
            
          	break;

          case Types.FLOAT:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
              byte[] val = new byte[4];
              ByteConverter.float4(val, 0, Float.parseFloat(String.valueOf(valueObject)));
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }

          case Types.DOUBLE:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
              byte[] val = new byte[8];
              ByteConverter.float8(val, 0, Double.parseDouble(String.valueOf(valueObject)));
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }
            
          	break;

          case Types.DECIMAL:
          case Types.NUMERIC:
            if (isBinary(columnIndex + 1)
            		&& valueObject != null) {
          	  Field field = fields[columnIndex];
          	  int mod = field.getMod();
          	  int serverPrecision;
          	  int serverScale;
          	  
          	  serverPrecision =  (mod == -1) 
          	  										? 0
          	  										: ((mod - 4) & 0xFFFF0000) >> 16;
          	    
          	  serverScale =  (mod == -1) 
          	  									? 0
          	  									: (mod - 4) & 0xFFFF;
            	
              byte[] val = ByteConverter.redshiftNumeric(new BigDecimal(String.valueOf(valueObject)), serverPrecision, serverScale);
	            rowBuffer.set(columnIndex, val);
            }
            else {
            	// Does as default switch case
              rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            }
            
          	break;
          	
          	
          default:
            rowBuffer.set(columnIndex, connection.encodeString(String.valueOf(valueObject)));
            break;
        }

      }
    }
  }

  public class CursorResultHandler extends ResultHandlerBase {

  	int resultsettype;

  	public CursorResultHandler(Properties inProps) {
  		this(0, inProps);
  	}
  	
  	public CursorResultHandler(int resultsettype, Properties inProps) {
      super(inProps);
      this.resultsettype = resultsettype;
  	}
  	
    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List<Tuple> tuples,
        ResultCursor cursor, RedshiftRowsBlockingQueue<Tuple> queueTuples,
        int[] rowCount, Thread ringBufferThread) {
      RedshiftResultSet.this.rows = tuples;
      RedshiftResultSet.this.cursor = cursor;
      RedshiftResultSet.this.queueRows = queueTuples;
      RedshiftResultSet.this.rowCount = rowCount;
      RedshiftResultSet.this.ringBufferThread = ringBufferThread;
    }

    @Override
    public void handleCommandStatus(String status, long updateCount, long insertOID) {
      handleError(new RedshiftException(GT.tr("Unexpected command status: {0}.", status),
          RedshiftState.PROTOCOL_VIOLATION));
    }

    @Override
    public void handleCompletion() throws SQLException {
      SQLWarning warning = getWarning();
      if (warning != null) {
        RedshiftResultSet.this.addWarning(warning);
      }
      super.handleCompletion();
    }
    
    @Override
    public boolean wantsScrollableResultSet() {
    	if(resultsettype !=0 )
    		return resultsettype != ResultSet.TYPE_FORWARD_ONLY;
    	else
    		return true; // Used in isLast() method.
    }
  }

  public BaseStatement getRedshiftStatement() {
    return statement;
  }

  //
  // Backwards compatibility with RedshiftRefCursorResultSet
  //

  private String refCursorName;

  public String getRefCursor() {
    // Can't check this because the RedshiftRefCursorResultSet
    // interface doesn't allow throwing a SQLException
    //
    // checkClosed();
    return refCursorName;
  }

  private void setRefCursor(String refCursorName) {
    this.refCursorName = refCursorName;
  }

  public void setFetchSize(int rows) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true, rows);
  	
    checkClosed();
    if (rows < 0) {
      throw new RedshiftException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
    fetchSize = rows;
  }

  public int getFetchSize() throws SQLException {
    checkClosed();
    return fetchSize;
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (onInsertRow) {
      throw new RedshiftException(GT.tr("Can''t use relative move methods while on the insert row."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (queueRows != null) {
      currentRow = 0;
      try {
				thisRow = queueRows.take();
				if (thisRow == null
						|| thisRow.fieldCount() == 0) {
					// End of result
					
					// Set suspended cursor, if any
					if(cursor == null)
						cursor = queueRows.getSuspendedPortal();
					
					// Check for any error
					resetBufAndCheckForAnyErrorInQueue();
	        
	        // Read more rows, if portal suspended
	        if(cursor != null
	        		&& queueRows.isSuspendedPortal()) {
	        	boolean moreRows = fetchMoreInQueueFromSuspendedPortal();
	        	if(!moreRows)
	        		return false;
	        } // Suspended portal
	        else
	        	return false; // End of the resultset.
				}
				else {
				//	System.out.print("R");
				}

			} catch (InterruptedException ie) {
        throw new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
            					RedshiftState.UNEXPECTED_ERROR, ie);
			}
    }
    else {
	    if (currentRow + 1 >= rows.size()) {
	      if (cursor == null || (maxRows > 0 && rowOffset + rows.size() >= maxRows)) {
	        currentRow = rows.size();
	        thisRow = null;
	        rowBuffer = null;
	        return false; // End of the resultset.
	      }
	
	      // Ask for some more data.
	      rowOffset += rows.size(); // We are discarding some data.
	
	      int fetchRows = fetchSize;
	      if (maxRows != 0) {
	        if (fetchRows == 0 || rowOffset + fetchRows > maxRows) {
	          // Fetch would exceed maxRows, limit it.
	          fetchRows = maxRows - rowOffset;
	        }
	      }
        Properties inProps = ((RedshiftConnectionImpl) connection).getConnectionProperties();
	      // Execute the fetch and update this resultset.
	      connection.getQueryExecutor().fetch(cursor, new CursorResultHandler(inProps), fetchRows, 0);
	
	      currentRow = 0;
	
	      // Test the new rows array.
	      if (rows.isEmpty()) {
	        thisRow = null;
	        rowBuffer = null;
	        return false;
	      }
	    } else {
	      currentRow++;
	    }
    }// !queueRows

    initRowBuffer();
    return true;
  }
  
  private void resetBufAndCheckForAnyErrorInQueue() throws SQLException, InterruptedException {
		SQLException ex = queueRows.getHandlerException();
		queueRows.addEndOfRowsIndicator(); // Keep End of result indicator for repeated next() call.
    rowBuffer = null;
    thisRow = null;
    if (ex != null)
    	throw ex;
  }
  
  private boolean fetchMoreInQueueFromSuspendedPortal() throws SQLException {
    long rowCount = getRowCount();
  	
    if ((maxRows > 0 && rowCount >= maxRows)) {
      return false; // End of the resultset.
    }
  	
    // Calculate fetch size based on max rows.
    int fetchRows = fetchSize;
    if (maxRows != 0) {
      if (fetchRows == 0 || rowCount + fetchRows > maxRows) {
        // Fetch would exceed maxRows, limit it.
        fetchRows = maxRows - (int)rowCount;
      }
    }
  	
    // Update statement state, so one can cancel the result fetch.
  	((RedshiftStatementImpl)statement).updateStatementCancleState(StatementCancelState.IDLE, StatementCancelState.IN_QUERY);
    
    // Execute the fetch and update this resultset.
    Properties inProps = ((RedshiftConnectionImpl) connection).getConnectionProperties();
    connection.getQueryExecutor().fetch(cursor, new CursorResultHandler(resultsettype, inProps), fetchRows, (int)rowCount);
    
    // We should get a new queue
    if (queueRows != null) {
      currentRow = 0;
      try {
				thisRow = queueRows.take();
				if (thisRow == null
						|| thisRow.fieldCount() == 0) {
					// End of result
					
			    // Update statement state.
			  	((RedshiftStatementImpl)statement).updateStatementCancleState(StatementCancelState.IN_QUERY, StatementCancelState.IDLE);
					
					// Check for any error
					resetBufAndCheckForAnyErrorInQueue();	  	  					
	        
	        return false;
				}
				else
					return true;
			} 
      catch (InterruptedException ie) {
        throw new RedshiftException(GT.tr("Interrupted exception retrieving query results."),
            					RedshiftState.UNEXPECTED_ERROR, ie);
			}
    } // Do we have queue?
    else
    	return false;
  }

  public void close() throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().logFunction(true);
  	
    try {
      closeInternally();
    } finally {
      ((RedshiftStatementImpl) statement).checkCompletion();
    }
    
    if (RedshiftLogger.isEnable()) {
    	connection.getLogger().logFunction(false);
    	connection.getLogger().flush();
    }
  }

  /*
  used by PgStatement.closeForNextExecution to avoid
  closing the firstUnclosedResult twice.
  checkCompletion above modifies firstUnclosedResult
  fixes issue #684
   */
  protected void closeInternally() throws SQLException {
    // release resources held (memory for tuples)
    rows = null;
    
    // Close ring buffer thread associated with this result, if any.
    connection.getQueryExecutor().closeRingBufferThread(queueRows, ringBufferThread);
    
    // release resources held (memory for queue)
    queueRows = null;
    rowCount = null;
    
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
  }

  public boolean wasNull() throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true);
  	
    checkClosed();
    return wasNullFlag;
  }

  private boolean isCharType(int columnIndex) throws SQLException {
  	int colType = getSQLType(columnIndex);
  	
  	return (colType == Types.VARCHAR
  						|| colType == Types.CHAR
  						|| colType == Types.LONGVARCHAR
  						|| colType == Types.NVARCHAR
  						|| colType == Types.NCHAR
  						|| colType == Types.LONGNVARCHAR
  						|| colType == Types.REF_CURSOR
  						|| (colType == Types.OTHER && !isInterval(columnIndex) &&
  								!isIntervalYearToMonth(columnIndex) &&
  								!isIntervalDayToSecond(columnIndex)));
  }
  
  @Override
  public String getString(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getString columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    // varchar in binary is same as text, other binary fields are converted to their text format
    if (isBinary(columnIndex) 
    			&& !isCharType(columnIndex)
    			&& !isGeometry(columnIndex)
    			&& !isGeometryHex(columnIndex)) {
      Field field = fields[columnIndex - 1];
      Object obj = internalGetObject(columnIndex, field);
      if (obj == null) {
        // internalGetObject() knows jdbc-types and some extra like hstore. It does not know of
        // RedshiftObject based types like geometric types but getObject does
        obj = getObject(columnIndex);
        if (obj == null) {
          return null;
        }
        return obj.toString();
      }
      // hack to be compatible with text protocol
      if (obj instanceof java.util.Date) {
        int oid = field.getOID();
        return connection.getTimestampUtils().timeToString((java.util.Date) obj,
            oid == Oid.TIMESTAMPTZ || oid == Oid.TIMETZ);
      }
      if (obj instanceof RedshiftIntervalYearToMonth) {
        RedshiftIntervalYearToMonth ym = (RedshiftIntervalYearToMonth) obj;
        return ym.getValue();
      }
      if (obj instanceof RedshiftIntervalDayToSecond) {
        RedshiftIntervalDayToSecond ds = (RedshiftIntervalDayToSecond) obj;
        return ds.getValue();
      }
      if ("hstore".equals(getRSType(columnIndex))) {
        return HStoreConverter.toString((Map<?, ?>) obj);
      }
      else
      if(isVarbyte(columnIndex)) {
      	// Convert raw binary to HEX
  	   return trimString(columnIndex, RedshiftVarbyte.convertToString((byte[])obj));
      } // VARBYTE
      else
      if(isGeography(columnIndex)) {
        // Convert raw binary to HEX
       return trimString(columnIndex, RedshiftGeography.convertToString((byte[])obj));
      } // GEOGRAPHY
      
      return trimString(columnIndex, obj.toString());
    }
    
    if (isGeometry(columnIndex) 
    		|| isGeometryHex(columnIndex)) {
      Field field = fields[columnIndex - 1];
      Object obj = internalGetObject(columnIndex, field);
      byte[] colData;
      
      if (obj == null) {
        // internalGetObject() knows jdbc-types and some extra like hstore. It does not know of
        // RedshiftObject based types like geometric types but getObject does
        obj = getObject(columnIndex);
        if (obj == null) {
          return null;
        }
      }
      
      if(isGeometry(columnIndex)) 
      	colData = (byte[])obj;
      else {
      	byte[] ewktfData = (byte[])obj;
      	colData = RedshiftGeometry.transformEWKTFormat(ewktfData, 0, ewktfData.length);      	
      }
      
      return RedshiftGeometry.convertToString(colData);
    	
    } // Geometry
    else {
	    Encoding encoding = connection.getEncoding();
	    try {
	      String rc = trimString(columnIndex, encoding.decode(thisRow.get(columnIndex - 1)));
	      if (fields[columnIndex - 1].getOID() == Oid.FLOAT8) {
	      	// Convert values like 20.19999999 to 20.2
	      	Double val = toDouble(rc);
	      	return val.toString();
	      }
	      else
	      	return rc;
	      
	    } catch (IOException ioe) {
	      throw new RedshiftException(
	          GT.tr(
	              "Invalid character data was found.  This is most likely caused by stored data containing characters that are invalid for the character set the database was created in.  The most common example of this is storing 8bit data in a SQL_ASCII database."),
	          RedshiftState.DATA_ERROR, ioe);
	    }
    }
  }

  /**
   * <p>Retrieves the value of the designated column in the current row of this <code>ResultSet</code>
   * object as a <code>boolean</code> in the Java programming language.</p>
   *
   * <p>If the designated column has a Character datatype and is one of the following values: "1",
   * "true", "t", "yes", "y" or "on", a value of <code>true</code> is returned. If the designated
   * column has a Character datatype and is one of the following values: "0", "false", "f", "no",
   * "n" or "off", a value of <code>false</code> is returned. Leading or trailing whitespace is
   * ignored, and case does not matter.</p>
   *
   * <p>If the designated column has a Numeric datatype and is a 1, a value of <code>true</code> is
   * returned. If the designated column has a Numeric datatype and is a 0, a value of
   * <code>false</code> is returned.</p>
   *
   * @param columnIndex the first column is 1, the second is 2, ...
   * @return the column value; if the value is SQL <code>NULL</code>, the value returned is
   * <code>false</code>
   * @exception SQLException if the columnIndex is not valid; if a database access error occurs; if
   *     this method is called on a closed result set or is an invalid cast to boolean type.
   * @see <a href="https://www.postgresql.org/docs/current/static/datatype-boolean.html">PostgreSQL
   * Boolean Type</a>
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getBoolean columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return false; // SQL NULL
    }

    int col = columnIndex - 1;
    if (Oid.BOOL == fields[col].getOID()
    		|| Oid.BIT == fields[col].getOID()) {
      final byte[] v = thisRow.get(col);
      if (isBinary(columnIndex)) {
      	return (1 == v.length) && (1 == v[0]);      	
      }
      else {
      	return (1 == v.length) 
      					&& (116 == v[0] // 116 = 't'
      							|| 1 == v[0]
      							|| '1' == v[0]); 
      }
    }

    if (isBinary(columnIndex)) {
    	try {
	      return BooleanTypeUtil.castToBoolean(readDoubleValue(thisRow.get(col), fields[col].getOID(), "boolean", columnIndex));
    	}
    	catch (RedshiftException ex) {
    		// Try using getObject. The readDoubleValue() call fails, when a column type is VARCHAR. 
    		return BooleanTypeUtil.castToBoolean(getObject(columnIndex));
    	}
    }

    return BooleanTypeUtil.castToBoolean(getObject(columnIndex));
  }

  private static final BigInteger BYTEMAX = new BigInteger(Byte.toString(Byte.MAX_VALUE));
  private static final BigInteger BYTEMIN = new BigInteger(Byte.toString(Byte.MIN_VALUE));

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getByte columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      // there is no Oid for byte so must always do conversion from
      // some other numeric type
      return (byte) readLongValue(thisRow.get(col), fields[col].getOID(), Byte.MIN_VALUE,
          Byte.MAX_VALUE, "byte", columnIndex);
    }

    String s = getString(columnIndex);

    if (s != null) {
      s = s.trim();
      if (s.isEmpty()) {
        return 0;
      }
      try {
        // try the optimal parse
        return Byte.parseByte(s);
      } catch (NumberFormatException e) {
        // didn't work, assume the column is not a byte
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();

          int gt = i.compareTo(BYTEMAX);
          int lt = i.compareTo(BYTEMIN);

          if (gt > 0 || lt < 0) {
            throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "byte", s),
                RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.byteValue();
        } catch (NumberFormatException ex) {
          throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "byte", s),
              RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getShort columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT2) {
        return ByteConverter.int2(thisRow.get(col), 0);
      }
      return (short) readLongValue(thisRow.get(col), oid, Short.MIN_VALUE, Short.MAX_VALUE, "short", columnIndex);
    }

    return toShort(getFixedString(columnIndex));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getInt columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT4) {
        return ByteConverter.int4(thisRow.get(col), 0);
      }
      return (int) readLongValue(thisRow.get(col), oid, Integer.MIN_VALUE, Integer.MAX_VALUE, "int", columnIndex);
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        return getFastInt(columnIndex);
      } catch (NumberFormatException ex) {
      }
    }
    return toInt(getFixedString(columnIndex));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getLong columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.INT8) {
        return ByteConverter.int8(thisRow.get(col), 0);
      }
      return readLongValue(thisRow.get(col), oid, Long.MIN_VALUE, Long.MAX_VALUE, "long", columnIndex);
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        return getFastLong(columnIndex);
      } catch (NumberFormatException ex) {
      }
    }
    return toLong(getFixedString(columnIndex));
  }

  /**
   * A dummy exception thrown when fast byte[] to number parsing fails and no value can be returned.
   * The exact stack trace does not matter because the exception is always caught and is not visible
   * to users.
   */
  private static final NumberFormatException FAST_NUMBER_FAILED = new NumberFormatException() {

    // Override fillInStackTrace to prevent memory leak via Throwable.backtrace hidden field
    // The field is not observable via reflection, however when throwable contains stacktrace, it
    // does
    // hold strong references to user objects (e.g. classes -> classloaders), thus it might lead to
    // OutOfMemory conditions.
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  };

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toLong(String)}.
   */
  private long getFastLong(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = thisRow.get(columnIndex - 1);

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    long val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 19) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 18) {
        throw FAST_NUMBER_FAILED;
      }
    }

    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        throw FAST_NUMBER_FAILED;
      }

      val *= 10;
      val += b - '0';
    }

    if (neg) {
      val = -val;
    }

    return val;
  }

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toInt(String)}.
   */
  private int getFastInt(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = thisRow.get(columnIndex - 1);

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    int val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 10) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 9) {
        throw FAST_NUMBER_FAILED;
      }
    }

    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        throw FAST_NUMBER_FAILED;
      }

      val *= 10;
      val += b - '0';
    }

    if (neg) {
      val = -val;
    }

    return val;
  }

  /**
   * Optimised byte[] to number parser. This code does not handle null values, so the caller must do
   * checkResultSet and handle null values prior to calling this function.
   *
   * @param columnIndex The column to parse.
   * @return The parsed number.
   * @throws SQLException If an error occurs while fetching column.
   * @throws NumberFormatException If the number is invalid or the out of range for fast parsing.
   *         The value must then be parsed by {@link #toBigDecimal(String, int)}.
   */
  private BigDecimal getFastBigDecimal(int columnIndex) throws SQLException, NumberFormatException {

    byte[] bytes = thisRow.get(columnIndex - 1);

    if (bytes.length == 0) {
      throw FAST_NUMBER_FAILED;
    }

    int scale = 0;
    long val = 0;
    int start;
    boolean neg;
    if (bytes[0] == '-') {
      neg = true;
      start = 1;
      if (bytes.length == 1 || bytes.length > 19) {
        throw FAST_NUMBER_FAILED;
      }
    } else {
      start = 0;
      neg = false;
      if (bytes.length > 18) {
        throw FAST_NUMBER_FAILED;
      }
    }

    int periodsSeen = 0;
    while (start < bytes.length) {
      byte b = bytes[start++];
      if (b < '0' || b > '9') {
        if (b == '.') {
          scale = bytes.length - start;
          periodsSeen++;
          continue;
        } else {
          throw FAST_NUMBER_FAILED;
        }
      }
      val *= 10;
      val += b - '0';
    }

    int numNonSignChars = neg ? bytes.length - 1 : bytes.length;
    if (periodsSeen > 1 || periodsSeen == numNonSignChars) {
      throw FAST_NUMBER_FAILED;
    }

    if (neg) {
      val = -val;
    }

    return BigDecimal.valueOf(val, scale);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getFloat columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.FLOAT4) {
        return ByteConverter.float4(thisRow.get(col), 0);
      }
      return (float) readDoubleValue(thisRow.get(col), oid, "float", columnIndex);
    }

    return toFloat(getFixedString(columnIndex));
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getDouble columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return 0; // SQL NULL
    }

    if (isBinary(columnIndex)) {
      int col = columnIndex - 1;
      int oid = fields[col].getOID();
      if (oid == Oid.FLOAT8) {
        return ByteConverter.float8(thisRow.get(col), 0);
      }
      return readDoubleValue(thisRow.get(col), oid, "double", columnIndex);
    }

    return toDouble(getFixedString(columnIndex));
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getBigDecimal columnIndex: {0}", columnIndex);
    
    return (BigDecimal) getNumeric(columnIndex, scale, false);
  }

  private Number getRedshiftNumeric(int columnIndex) {
	  Field field = fields[columnIndex - 1];
	  int mod = field.getMod();
	  int serverPrecision;
	  int serverScale;
	  
	  serverPrecision =  (mod == -1) 
	  										? 0
	  										: ((mod - 4) & 0xFFFF0000) >> 16;
	    
	  serverScale =  (mod == -1) 
	  									? 0
	  									: (mod - 4) & 0xFFFF;
	    
	  
	  return ByteConverter.redshiftNumeric(thisRow.get(columnIndex - 1), serverPrecision, serverScale);
  }
  
  private Number getNumeric(int columnIndex, int scale, boolean allowNaN) throws SQLException {
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      int sqlType = getSQLType(columnIndex);
      if (sqlType != Types.NUMERIC && sqlType != Types.DECIMAL) {
        Object obj = internalGetObject(columnIndex, fields[columnIndex - 1]);
        if (obj == null) {
          return null;
        }
        if (obj instanceof Long || obj instanceof Integer || obj instanceof Byte) {
          BigDecimal res = BigDecimal.valueOf(((Number) obj).longValue());
          res = scaleBigDecimal(res, scale);
          return res;
        }
        return toBigDecimal(trimMoney(String.valueOf(obj)), scale);
      } else {
//        Number num = ByteConverter.numeric(thisRow.get(columnIndex - 1));
      	Number num = getRedshiftNumeric(columnIndex);

        if (allowNaN && Double.isNaN(num.doubleValue())) {
          return Double.NaN;
        }

        return num;
      }
    }

    Encoding encoding = connection.getEncoding();
    if (encoding.hasAsciiNumbers()) {
      try {
        BigDecimal res = getFastBigDecimal(columnIndex);
        res = scaleBigDecimal(res, scale);
        return res;
      } catch (NumberFormatException ignore) {
      }
    }

    String stringValue = getFixedString(columnIndex);
    if (allowNaN && "NaN".equalsIgnoreCase(stringValue)) {
      return Double.NaN;
    }
    return toBigDecimal(stringValue, scale);
  }

  /**
   * {@inheritDoc}
   *
   * <p>In normal use, the bytes represent the raw values returned by the backend. However, if the
   * column is an OID, then it is assumed to refer to a Large Object, and that object is returned as
   * a byte array.</p>
   *
   * <p><b>Be warned</b> If the large object is huge, then you may run out of memory.</p>
   */
  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getBytes columnIndex: {0}", columnIndex);
    
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      // If the data is already binary then just return it
      return trimBytes(columnIndex, thisRow.get(columnIndex - 1));
    }
    if (fields[columnIndex - 1].getOID() == Oid.BYTEA) {
      return trimBytes(columnIndex, RedshiftBytea.toBytes(thisRow.get(columnIndex - 1)));
    }
    else
    if (fields[columnIndex - 1].getOID() == Oid.VARBYTE) {
      return trimBytes(columnIndex, RedshiftVarbyte.toBytes(thisRow.get(columnIndex - 1)));
    } 
    else
    if (fields[columnIndex - 1].getOID() == Oid.GEOGRAPHY) {
      return trimBytes(columnIndex, RedshiftGeography.toBytes(thisRow.get(columnIndex - 1)));
    } 
    else {
      return trimBytes(columnIndex, thisRow.get(columnIndex - 1));
    }
  }

  public java.sql.Date getDate(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getDate columnIndex: {0}", columnIndex);
    return getDate(columnIndex, null);
  }

  public Time getTime(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getTime columnIndex: {0}", columnIndex);
    return getTime(columnIndex, null);
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getTimestamp columnIndex: {0}", columnIndex);
    return getTimestamp(columnIndex, null);
  }

  public RedshiftIntervalYearToMonth getIntervalYearToMonth(int columnIndex) throws SQLException{
    if (RedshiftLogger.isEnable())
      connection.getLogger().log(LogLevel.DEBUG, "  getIntervalYearToMonth columnIndex: {0}", columnIndex);

    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      return new RedshiftIntervalYearToMonth(ByteConverter.int4(thisRow.get(columnIndex - 1), 0));
    }

    String str = getString(columnIndex);
    return new RedshiftIntervalYearToMonth(getString(columnIndex));
  }

  public RedshiftIntervalDayToSecond getIntervalDayToSecond(int columnIndex) throws SQLException{
    if (RedshiftLogger.isEnable())
      connection.getLogger().log(LogLevel.DEBUG, "  getIntervalDayToSecond columnIndex: {0}", columnIndex);

    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    if (isBinary(columnIndex)) {
      return new RedshiftIntervalDayToSecond(ByteConverter.int8(thisRow.get(columnIndex - 1), 0));
    }

    String str = getString(columnIndex);
    return new RedshiftIntervalDayToSecond(getString(columnIndex));
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "  getAsciiStream columnIndex: {0}", columnIndex);
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    // Version 7.2 supports AsciiStream for all the RS text types
    // As the spec/javadoc for this method indicate this is to be used for
    // large text values (i.e. LONGVARCHAR) RS doesn't have a separate
    // long string datatype, but with toast the text datatype is capable of
    // handling very large values. Thus the implementation ends up calling
    // getString() since there is no current way to stream the value from the server
    try {
      return new ByteArrayInputStream(getString(columnIndex).getBytes("ASCII"));
    } catch (UnsupportedEncodingException l_uee) {
      throw new RedshiftException(GT.tr("The JVM claims not to support the encoding: {0}", "ASCII"),
          RedshiftState.UNEXPECTED_ERROR, l_uee);
    }
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getUnicodeStream columnIndex: {0}", columnIndex);
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    // Version 7.2 supports AsciiStream for all the RS text types
    // As the spec/javadoc for this method indicate this is to be used for
    // large text values (i.e. LONGVARCHAR) RS doesn't have a separate
    // long string datatype, but with toast the text datatype is capable of
    // handling very large values. Thus the implementation ends up calling
    // getString() since there is no current way to stream the value from the server
    try {
      return new ByteArrayInputStream(getString(columnIndex).getBytes("UTF-8"));
    } catch (UnsupportedEncodingException l_uee) {
      throw new RedshiftException(GT.tr("The JVM claims not to support the encoding: {0}", "UTF-8"),
          RedshiftState.UNEXPECTED_ERROR, l_uee);
    }
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getBinaryStream columnIndex: {0}", columnIndex);
    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    // Version 7.2 supports BinaryStream for all RS bytea type
    // As the spec/javadoc for this method indicate this is to be used for
    // large binary values (i.e. LONGVARBINARY) RS doesn't have a separate
    // long binary datatype, but with toast the bytea datatype is capable of
    // handling very large values. Thus the implementation ends up calling
    // getBytes() since there is no current way to stream the value from the server
    byte[] b = getBytes(columnIndex);
    if (b != null) {
      return new ByteArrayInputStream(b);
    }
    return null;
  }

  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  public byte getByte(String columnName) throws SQLException {

    return getByte(findColumn(columnName));
  }

  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  public byte[] getBytes(String columnName) throws SQLException {
    return getBytes(findColumn(columnName));
  }

  public java.sql.Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName), null);
  }

  public Time getTime(String columnName) throws SQLException {
    return getTime(findColumn(columnName), null);
  }

  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName), null);
  }

  public RedshiftIntervalYearToMonth getIntervalYearToMonth(String columnName) throws SQLException {
    return getIntervalYearToMonth(findColumn(columnName));
  }

  public RedshiftIntervalDayToSecond getIntervalDayToSecond(String columnName) throws SQLException {
    return getIntervalDayToSecond(findColumn(columnName));
  }

  public InputStream getAsciiStream(String columnName) throws SQLException {
    return getAsciiStream(findColumn(columnName));
  }

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    return getUnicodeStream(findColumn(columnName));
  }

  public InputStream getBinaryStream(String columnName) throws SQLException {
    return getBinaryStream(findColumn(columnName));
  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    if (warningChain == null) {
      return null;
    }
    return warningChain.getFirstWarning();
  }

  public void clearWarnings() throws SQLException {
    checkClosed();
    warningChain = null;
  }

  protected void addWarning(SQLWarning warnings) throws RedshiftException {
    if (this.warningChain != null) {
      this.warningChain.appendWarning(warnings);
    } else {
      this.warningChain = new RedshiftWarningWrapper(warnings, ((RedshiftConnectionImpl) this.connection).getConnectionProperties());
    }
  }

  public String getCursorName() throws SQLException {
    checkClosed();
    return null;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getObject columnIndex: {0}", columnIndex);
    Field field;

    checkResultSet(columnIndex);
    if (wasNullFlag) {
      return null;
    }

    field = fields[columnIndex - 1];

    // some fields can be null, mainly from those returned by MetaData methods
    if (field == null) {
      wasNullFlag = true;
      return null;
    }

    Object result = internalGetObject(columnIndex, field);
    if (result != null) {
      return result;
    }

    if (isBinary(columnIndex)) {
      return connection.getObject(getRSType(columnIndex), null, thisRow.get(columnIndex - 1));
    }
    return connection.getObject(getRSType(columnIndex), getString(columnIndex), null);
  }

  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  public int findColumn(String columnName) throws SQLException {
    checkClosed();

    int col = findColumnIndex(columnName);
    if (col == 0) {
      throw new RedshiftException(
          GT.tr("The column name {0} was not found in this ResultSet.", columnName),
          RedshiftState.UNDEFINED_COLUMN);
    }
    return col;
  }

  public static Map<String, Integer> createColumnNameIndexMap(Field[] fields,
      boolean isSanitiserDisabled) {
    Map<String, Integer> columnNameIndexMap = new HashMap<String, Integer>(fields.length * 2);
    // The JDBC spec says when you have duplicate columns names,
    // the first one should be returned. So load the map in
    // reverse order so the first ones will overwrite later ones.
    for (int i = fields.length - 1; i >= 0; i--) {
      String columnLabel = fields[i].getColumnLabel();
      if (isSanitiserDisabled) {
        columnNameIndexMap.put(columnLabel, i + 1);
      } else {
        columnNameIndexMap.put(columnLabel.toLowerCase(Locale.US), i + 1);
      }
    }
    return columnNameIndexMap;
  }

  private int findColumnIndex(String columnName) {
    if (columnNameIndexMap == null) {
      if (originalQuery != null) {
        columnNameIndexMap = originalQuery.getResultSetColumnNameIndexMap();
      }
      if (columnNameIndexMap == null) {
        columnNameIndexMap = createColumnNameIndexMap(fields, connection.isColumnSanitiserDisabled());
      }
    }

    Integer index = columnNameIndexMap.get(columnName);
    if (index != null) {
      return index;
    }

    index = columnNameIndexMap.get(columnName.toLowerCase(Locale.US));
    if (index != null) {
      columnNameIndexMap.put(columnName, index);
      return index;
    }

    index = columnNameIndexMap.get(columnName.toUpperCase(Locale.US));
    if (index != null) {
      columnNameIndexMap.put(columnName, index);
      return index;
    }

    return 0;
  }

  /**
   * Returns the OID of a field. It is used internally by the driver.
   *
   * @param field field index
   * @return OID of a field
   */
  public int getColumnOID(int field) {
    return fields[field - 1].getOID();
  }

  /**
   * <p>This is used to fix get*() methods on Money fields. It should only be used by those methods!</p>
   *
   * <p>It converts ($##.##) to -##.## and $##.## to ##.##</p>
   *
   * @param col column position (1-based)
   * @return numeric-parsable representation of money string literal
   * @throws SQLException if something wrong happens
   */
  public String getFixedString(int col) throws SQLException {
    return trimMoney(getString(col));
  }

  private String trimMoney(String s) {
    if (s == null) {
      return null;
    }

    // if we don't have at least 2 characters it can't be money.
    if (s.length() < 2) {
      return s;
    }

    // Handle Money
    char ch = s.charAt(0);

    // optimise for non-money type: return immediately with one check
    // if the first char cannot be '(', '$' or '-'
    if (ch > '-') {
      return s;
    }

    if (ch == '(') {
      s = "-" + RedshiftTokenizer.removePara(s).substring(1);
    } else if (ch == '$') {
      s = s.substring(1);
    } else if (ch == '-' && s.charAt(1) == '$') {
      s = "-" + s.substring(2);
    }

    return s;
  }

  protected String getRSType(int column) throws SQLException {
    Field field = fields[column - 1];
    initSqlType(field);
    return field.getRSType();
  }

  protected int getSQLType(int column) throws SQLException {
    Field field = fields[column - 1];
    initSqlType(field);
    return field.getSQLType();
  }

  private void initSqlType(Field field) throws SQLException {
    if (field.isTypeInitialized()) {
      return;
    }
    TypeInfo typeInfo = connection.getTypeInfo();
    int oid = field.getOID();
    String pgType = typeInfo.getRSType(oid);
    int sqlType = typeInfo.getSQLType(pgType);
    field.setSQLType(sqlType);
    field.setRSType(pgType);
  }

  private void checkUpdateable() throws SQLException {
    checkClosed();

    if (!isUpdateable()) {
      throw new RedshiftException(
          GT.tr(
              "ResultSet is not updateable.  The query that generated this result set must select only one table, and must select all primary keys from that table. See the JDBC 2.1 API Specification, section 5.6 for more details."),
          RedshiftState.INVALID_CURSOR_STATE);
    }

    if (updateValues == null) {
      // allow every column to be updated without a rehash.
      updateValues = new HashMap<String, Object>((int) (fields.length / 0.75), 0.75f);
    }
  }

  protected void checkClosed() throws SQLException {
    if (rows == null && queueRows == null) {
      throw new RedshiftException(GT.tr("This ResultSet is closed."), RedshiftState.OBJECT_NOT_IN_STATE);
    }
  }

  /*
   * for jdbc3 to call internally
   */
  protected boolean isResultSetClosed() {
    return rows == null && queueRows == null;
  }

  protected void checkColumnIndex(int column) throws SQLException {
    if (column < 1 || column > fields.length) {
      throw new RedshiftException(
          GT.tr("The column index is out of range: {0}, number of columns: {1}.",
              column, fields.length),
          RedshiftState.INVALID_PARAMETER_VALUE);
    }
  }

  /**
   * Checks that the result set is not closed, it's positioned on a valid row and that the given
   * column number is valid. Also updates the {@link #wasNullFlag} to correct value.
   *
   * @param column The column number to check. Range starts from 1.
   * @throws SQLException If state or column is invalid.
   */
  protected void checkResultSet(int column) throws SQLException {
    checkClosed();
    if (thisRow == null) {
      throw new RedshiftException(
          GT.tr("ResultSet not positioned properly, perhaps you need to call next."),
          RedshiftState.INVALID_CURSOR_STATE);
    }
    checkColumnIndex(column);
    wasNullFlag = (thisRow.get(column - 1) == null);
  }

  /**
   * Returns true if the value of the given column is in binary format.
   *
   * @param column The column to check. Range starts from 1.
   * @return True if the column is in binary format.
   */
  protected boolean isBinary(int column) {
    return fields[column - 1].getFormat() == Field.BINARY_FORMAT;
  }

  protected boolean isGeometry(int column) {
    return (fields[column - 1].getOID() == Oid.GEOMETRY);
  }

  protected boolean isGeometryHex(int column) {
    return (fields[column - 1].getOID() == Oid.GEOMETRYHEX);
  }

  protected boolean isSuper(int column) {
    return (fields[column - 1].getOID() == Oid.SUPER);
  }

  protected boolean isVarbyte(int column) {
    return (fields[column - 1].getOID() == Oid.VARBYTE);
  }

  protected boolean isGeography(int column) {
    return (fields[column - 1].getOID() == Oid.GEOGRAPHY);
  }
  
  protected boolean isInterval(int column) {
    return (fields[column - 1].getOID() == Oid.INTERVAL);
  }

  protected boolean isIntervalYearToMonth(int column) {
    return (fields[column - 1].getOID() == Oid.INTERVALY2M);
  }

  protected boolean isIntervalDayToSecond(int column) {
    return (fields[column - 1].getOID() == Oid.INTERVALD2S);
  }
  
  
  // ----------------- Formatting Methods -------------------

  private static final BigInteger SHORTMAX = new BigInteger(Short.toString(Short.MAX_VALUE));
  private static final BigInteger SHORTMIN = new BigInteger(Short.toString(Short.MIN_VALUE));

  public static short toShort(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Short.parseShort(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(SHORTMAX);
          int lt = i.compareTo(SHORTMIN);

          if (gt > 0 || lt < 0) {
            throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "short", s),
                RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.shortValue();

        } catch (NumberFormatException ne) {
          throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "short", s),
              RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  private static final BigInteger INTMAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
  private static final BigInteger INTMIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));

  public static int toInt(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();

          int gt = i.compareTo(INTMAX);
          int lt = i.compareTo(INTMIN);

          if (gt > 0 || lt < 0) {
            throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "int", s),
                RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.intValue();

        } catch (NumberFormatException ne) {
          throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "int", s),
              RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  private static final BigInteger LONGMAX = new BigInteger(Long.toString(Long.MAX_VALUE));
  private static final BigInteger LONGMIN = new BigInteger(Long.toString(Long.MIN_VALUE));

  public static long toLong(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Long.parseLong(s);
      } catch (NumberFormatException e) {
        try {
          BigDecimal n = new BigDecimal(s);
          BigInteger i = n.toBigInteger();
          int gt = i.compareTo(LONGMAX);
          int lt = i.compareTo(LONGMIN);

          if (gt > 0 || lt < 0) {
            throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "long", s),
                RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
          }
          return i.longValue();
        } catch (NumberFormatException ne) {
          throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "long", s),
              RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
      }
    }
    return 0; // SQL NULL
  }

  public static BigDecimal toBigDecimal(String s) throws SQLException {
    if (s == null) {
      return null;
    }
    try {
      s = s.trim();
      return new BigDecimal(s);
    } catch (NumberFormatException e) {
      throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "BigDecimal", s),
          RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
  }

  public BigDecimal toBigDecimal(String s, int scale) throws SQLException {
    if (s == null) {
      return null;
    }
    BigDecimal val = toBigDecimal(s);
    return scaleBigDecimal(val, scale);
  }

  private BigDecimal scaleBigDecimal(BigDecimal val, int scale) throws RedshiftException {
    if (scale == -1) {
      return val;
    }
    try {
      return val.setScale(scale);
    } catch (ArithmeticException e) {
      throw new RedshiftException(
          GT.tr("Bad value for type {0} : {1}", "BigDecimal", val),
          RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
  }

  public static float toFloat(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Float.parseFloat(s);
      } catch (NumberFormatException e) {
        throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "float", s),
            RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  public static double toDouble(String s) throws SQLException {
    if (s != null) {
      try {
        s = s.trim();
        return Double.parseDouble(s);
      } catch (NumberFormatException e) {
        throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", "double", s),
            RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
      }
    }
    return 0; // SQL NULL
  }

  private void initRowBuffer() {
  	if (queueRows == null)
  		thisRow = rows.get(currentRow);
    // We only need a copy of the current row if we're going to
    // modify it via an updatable resultset.
    if (resultsetconcurrency == ResultSet.CONCUR_UPDATABLE) {
    	if (thisRow != null)
    		rowBuffer = thisRow.updateableCopy();
    	else
        rowBuffer = null;
    } else {
      rowBuffer = null;
    }
  }

  private boolean isColumnTrimmable(int columnIndex) throws SQLException {
    switch (getSQLType(columnIndex)) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return true;
    }
    return false;
  }

  private byte[] trimBytes(int columnIndex, byte[] bytes) throws SQLException {
    // we need to trim if maxsize is set and the length is greater than maxsize and the
    // type of this column is a candidate for trimming
    if (maxFieldSize > 0 && bytes.length > maxFieldSize && isColumnTrimmable(columnIndex)) {
      byte[] newBytes = new byte[maxFieldSize];
      System.arraycopy(bytes, 0, newBytes, 0, maxFieldSize);
      return newBytes;
    } else {
      return bytes;
    }
  }

  private String trimString(int columnIndex, String string) throws SQLException {
    // we need to trim if maxsize is set and the length is greater than maxsize and the
    // type of this column is a candidate for trimming
    if (maxFieldSize > 0 && string.length() > maxFieldSize && isColumnTrimmable(columnIndex)) {
      return string.substring(0, maxFieldSize);
    } else {
      return string;
    }
  }

  /**
   * Converts any numeric binary field to double value. This method does no overflow checking.
   *
   * @param bytes The bytes of the numeric field.
   * @param oid The oid of the field.
   * @param targetType The target type. Used for error reporting.
   * @return The value as double.
   * @throws RedshiftException If the field type is not supported numeric type.
   */
  private double readDoubleValue(byte[] bytes, int oid, String targetType, int columnIndex) throws RedshiftException {
    // currently implemented binary encoded fields
    switch (oid) {
      case Oid.INT2:
        return ByteConverter.int2(bytes, 0);
      case Oid.INT4:
        return ByteConverter.int4(bytes, 0);
      case Oid.INT8:
        // might not fit but there still should be no overflow checking
        return ByteConverter.int8(bytes, 0);
      case Oid.FLOAT4:
        return ByteConverter.float4(bytes, 0);
      case Oid.FLOAT8:
        return ByteConverter.float8(bytes, 0);
      case Oid.NUMERIC:
//        return ByteConverter.numeric(bytes).doubleValue();
      	return getRedshiftNumeric(columnIndex).doubleValue();
    }
    throw new RedshiftException(GT.tr("Cannot convert the column of type {0} to requested type {1}.",
        Oid.toString(oid), targetType), RedshiftState.DATA_TYPE_MISMATCH);
  }

  /**
   * <p>Converts any numeric binary field to long value.</p>
   *
   * <p>This method is used by getByte,getShort,getInt and getLong. It must support a subset of the
   * following java types that use Binary encoding. (fields that use text encoding use a different
   * code path).
   *
   * <code>byte,short,int,long,float,double,BigDecimal,boolean,string</code>.
   * </p>
   *
   * @param bytes The bytes of the numeric field.
   * @param oid The oid of the field.
   * @param minVal the minimum value allowed.
   * @param maxVal the maximum value allowed.
   * @param targetType The target type. Used for error reporting.
   * @return The value as long.
   * @throws RedshiftException If the field type is not supported numeric type or if the value is out of
   *         range.
   */
  private long readLongValue(byte[] bytes, int oid, long minVal, long maxVal, String targetType, int columnIndex)
      throws RedshiftException {
    long val = 0;
    // currently implemented binary encoded fields
    switch (oid) {
      case Oid.INT2:
        val = ByteConverter.int2(bytes, 0);
        break;
      case Oid.INT4:
        val = ByteConverter.int4(bytes, 0);
        break;
      case Oid.OID:
        if(bytes.length == 4){
          val = ByteConverter.int4(bytes, 0);
        }
        else if(bytes.length == 8){
          val = ByteConverter.int8(bytes, 0);
        }
        break;
      case Oid.INT8:
      case Oid.XIDOID:
        val = ByteConverter.int8(bytes, 0);
        break;
      case Oid.FLOAT4:
        val = (long) ByteConverter.float4(bytes, 0);
        break;
      case Oid.FLOAT8:
        val = (long) ByteConverter.float8(bytes, 0);
        break;
      case Oid.NUMERIC:
//        Number num = ByteConverter.numeric(bytes);
      	Number num = getRedshiftNumeric(columnIndex);
        
        if (num instanceof  BigDecimal) {
          val = ((BigDecimal) num).setScale(0 , RoundingMode.DOWN).longValueExact();
        } else {
          val = num.longValue();
        }
        break;
      default:
        throw new RedshiftException(
            GT.tr("Cannot convert the column of type {0} to requested type {1}.",
                Oid.toString(oid), targetType),
            RedshiftState.DATA_TYPE_MISMATCH);
    }
    if (val < minVal || val > maxVal) {
      throw new RedshiftException(GT.tr("Bad value for type {0} : {1}", targetType, val),
          RedshiftState.NUMERIC_VALUE_OUT_OF_RANGE);
    }
    return val;
  }

  protected void updateValue(int columnIndex, Object value) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().logFunction(true, columnIndex, value);

  	checkUpdateable();

  	if(queueRows != null) {
	  	throw new RedshiftException(GT.tr("Cannot call updateValue() when enableFetchRingBuffer is true."),
	      RedshiftState.INVALID_CURSOR_STATE);
  	} else if (!onInsertRow && (isBeforeFirst() || isAfterLast() || rows.isEmpty())) {
	      throw new RedshiftException(
	          GT.tr(
	              "Cannot update the ResultSet because it is either before the start or after the end of the results."),
	          RedshiftState.INVALID_CURSOR_STATE);
    }

    checkColumnIndex(columnIndex);

    doingUpdates = !onInsertRow;
    if (value == null) {
      updateNull(columnIndex);
    } else {
      RedshiftResultSetMetaData md = (RedshiftResultSetMetaData) getMetaData();
      updateValues.put(md.getBaseColumnName(columnIndex), value);
    }
  }

  protected Object getUUID(String data) throws SQLException {
    UUID uuid;
    try {
      uuid = UUID.fromString(data);
    } catch (IllegalArgumentException iae) {
      throw new RedshiftException(GT.tr("Invalid UUID data."), RedshiftState.INVALID_PARAMETER_VALUE, iae);
    }

    return uuid;
  }

  protected Object getUUID(byte[] data) throws SQLException {
    return new UUID(ByteConverter.int8(data, 0), ByteConverter.int8(data, 8));
  }

  private class PrimaryKey {
    int index; // where in the result set is this primaryKey
    String name; // what is the columnName of this primary Key

    PrimaryKey(int index, String name) {
      this.index = index;
      this.name = name;
    }

    Object getValue() throws SQLException {
      return getObject(index);
    }
  }

  //
  // We need to specify the type of NULL when updating a column to NULL, so
  // NullObject is a simple extension of RedshiftObject that always returns null
  // values but retains column type info.
  //

  static class NullObject extends RedshiftObject {
    NullObject(String type) {
      setType(type);
    }

    public String getValue() {
      return null;
    }
  }

  /**
   * Used to add rows to an already existing ResultSet that exactly match the existing rows.
   * Currently only used for assembling generated keys from batch statement execution.
   */
  void addRows(List<Tuple> tuples) {
    rows.addAll(tuples);
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateRef(int,Ref)");
  }

  public void updateRef(String columnName, Ref x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateRef(String,Ref)");
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateBlob(int,Blob)");
  }

  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateBlob(String,Blob)");
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateClob(int,Clob)");
  }

  public void updateClob(String columnName, Clob x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateClob(String,Clob)");
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    updateObject(columnIndex, x);
  }

  public void updateArray(String columnName, Array x) throws SQLException {
    updateArray(findColumn(columnName), x);
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if (type == null) {
      throw new SQLException("type is null");
    }
    int sqlType = getSQLType(columnIndex);
    if (type == BigDecimal.class) {
      if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
        return type.cast(getBigDecimal(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == String.class) {
      if (sqlType == Types.CHAR || sqlType == Types.VARCHAR) {
        return type.cast(getString(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Boolean.class) {
      if (sqlType == Types.BOOLEAN || sqlType == Types.BIT) {
        boolean booleanValue = getBoolean(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(booleanValue);
      }
      if (sqlType == Types.DATE 
      		|| sqlType == Types.TIME
      		|| sqlType == Types.TIMESTAMP
      		//JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
      		|| sqlType == Types.TIME_WITH_TIMEZONE
      		|| sqlType == Types.TIMESTAMP_WITH_TIMEZONE
      		//JCP! endif
      		|| sqlType == Types.BINARY) {
        if (wasNull()) {
          return null;
        }
      	
	      throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
	      RedshiftState.INVALID_PARAMETER_VALUE);
      }
      else {
      	String booleanStrValue = getString(columnIndex);
        if (wasNull()) {
          return null;
        }
      	return type.cast(BooleanTypeUtil.castToBoolean(booleanStrValue));      	
//        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
//                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Short.class) {
      if (sqlType == Types.SMALLINT) {
        short shortValue = getShort(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(shortValue);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Integer.class) {
      if (sqlType == Types.INTEGER || sqlType == Types.SMALLINT) {
        int intValue = getInt(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(intValue);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Long.class) {
      if (sqlType == Types.BIGINT) {
        long longValue = getLong(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(longValue);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == BigInteger.class) {
      if (sqlType == Types.BIGINT) {
        long longValue = getLong(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(BigInteger.valueOf(longValue));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Float.class) {
      if (sqlType == Types.REAL) {
        float floatValue = getFloat(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(floatValue);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Double.class) {
      if (sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
        double doubleValue = getDouble(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(doubleValue);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Date.class) {
      if (sqlType == Types.DATE) {
        return type.cast(getDate(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Time.class) {
      if (sqlType == Types.TIME
            //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
            || sqlType == Types.TIME_WITH_TIMEZONE
            //JCP! endif
      ) {
        return type.cast(getTime(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Timestamp.class) {
      if (sqlType == Types.TIMESTAMP
              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
              || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
      //JCP! endif
      ) {
        return type.cast(getTimestamp(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Calendar.class) {
      if (sqlType == Types.TIMESTAMP
              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
              || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
      //JCP! endif
      ) {
        Timestamp timestampValue = getTimestamp(columnIndex);
        if (wasNull()) {
          return null;
        }
        Calendar calendar = Calendar.getInstance(getDefaultCalendar().getTimeZone());
        calendar.setTimeInMillis(timestampValue.getTime());
        return type.cast(calendar);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Blob.class) {
      if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.BIGINT) {
        return type.cast(getBlob(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Clob.class) {
      if (sqlType == Types.CLOB || sqlType == Types.BIGINT) {
        return type.cast(getClob(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == java.util.Date.class) {
      if (sqlType == Types.TIMESTAMP
              //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
              || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
              //JCP! endif
      ) {
        Timestamp timestamp = getTimestamp(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(new java.util.Date(timestamp.getTime()));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == Array.class) {
      if (sqlType == Types.ARRAY) {
        return type.cast(getArray(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == SQLXML.class) {
      if (sqlType == Types.SQLXML) {
        return type.cast(getSQLXML(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == UUID.class) {
      return type.cast(getObject(columnIndex));
    } else if (type == InetAddress.class) {
      String inetText = getString(columnIndex);
      if (inetText == null) {
        return null;
      }
      int slash = inetText.indexOf("/");
      try {
        return type.cast(InetAddress.getByName(slash < 0 ? inetText : inetText.substring(0, slash)));
      } catch (UnknownHostException ex) {
        throw new RedshiftException(GT.tr("Invalid Inet data."), RedshiftState.INVALID_PARAMETER_VALUE, ex);
      }
      // JSR-310 support
      //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
    } else if (type == LocalDate.class) {
      if (sqlType == Types.DATE) {
        Date dateValue = getDate(columnIndex);
        if (wasNull()) {
          return null;
        }
        long time = dateValue.getTime();
        if (time == RedshiftStatement.DATE_POSITIVE_INFINITY) {
          return type.cast(LocalDate.MAX);
        }
        if (time == RedshiftStatement.DATE_NEGATIVE_INFINITY) {
          return type.cast(LocalDate.MIN);
        }
        return type.cast(dateValue.toLocalDate());
      } else if (sqlType == Types.TIMESTAMP) {
        LocalDateTime localDateTimeValue = getLocalDateTime(columnIndex);
        if (wasNull()) {
          return null;
        }
        return type.cast(localDateTimeValue.toLocalDate());
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == LocalTime.class) {
      if (sqlType == Types.TIME) {
        return type.cast(getLocalTime(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == LocalDateTime.class) {
      if (sqlType == Types.TIMESTAMP) {
        return type.cast(getLocalDateTime(columnIndex));
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
    } else if (type == OffsetDateTime.class) {
      if (sqlType == Types.TIMESTAMP_WITH_TIMEZONE || sqlType == Types.TIMESTAMP) {
        OffsetDateTime offsetDateTime = getOffsetDateTime(columnIndex);
        return type.cast(offsetDateTime);
      } else {
        throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
                RedshiftState.INVALID_PARAMETER_VALUE);
      }
      //JCP! endif
    } else if (RedshiftObject.class.isAssignableFrom(type)) {
      Object object;
      if (isBinary(columnIndex)) {
        object = connection.getObject(getRSType(columnIndex), null, thisRow.get(columnIndex - 1));
      } else {
        object = connection.getObject(getRSType(columnIndex), getString(columnIndex), null);
      }
      return type.cast(object);
    }
    throw new RedshiftException(GT.tr("conversion to {0} from {1} not supported", type, getRSType(columnIndex)),
            RedshiftState.INVALID_PARAMETER_VALUE);
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(s, map);
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(i, map);
  }

  //JCP! if mvn.project.property.redshift.jdbc.spec >= "JDBC4.2"
  public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType,
      int scaleOrLength) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType,
      int scaleOrLength) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(int columnIndex, Object x, java.sql.SQLType targetSqlType)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateObject");
  }

  public void updateObject(String columnLabel, Object x, java.sql.SQLType targetSqlType)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateObject");
  }
  //JCP! endif

  public RowId getRowId(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable()) 
    	connection.getLogger().log(LogLevel.DEBUG, "  getRowId columnIndex: {0}", columnIndex);
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getRowId(int)");
  }

  public RowId getRowId(String columnName) throws SQLException {
    return getRowId(findColumn(columnName));
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateRowId(int, RowId)");
  }

  public void updateRowId(String columnName, RowId x) throws SQLException {
    updateRowId(findColumn(columnName), x);
  }

  public int getHoldability() throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getHoldability()");
  }

  public boolean isClosed() throws SQLException {
    return (rows == null && queueRows == null);
  }

  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateNString(int, String)");
  }

  public void updateNString(String columnName, String nString) throws SQLException {
    updateNString(findColumn(columnName), nString);
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateNClob(int, NClob)");
  }

  public void updateNClob(String columnName, NClob nClob) throws SQLException {
    updateNClob(findColumn(columnName), nClob);
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateNClob(int, Reader)");
  }

  public void updateNClob(String columnName, Reader reader) throws SQLException {
    updateNClob(findColumn(columnName), reader);
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateNClob(int, Reader, long)");
  }

  public void updateNClob(String columnName, Reader reader, long length) throws SQLException {
    updateNClob(findColumn(columnName), reader, length);
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getNClob columnIndex: {0}", columnIndex);
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getNClob(int)");
  }

  public NClob getNClob(String columnName) throws SQLException {
    return getNClob(findColumn(columnName));
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateBlob(int, InputStream, long)");
  }

  public void updateBlob(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateBlob(findColumn(columnName), inputStream, length);
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateBlob(int, InputStream)");
  }

  public void updateBlob(String columnName, InputStream inputStream) throws SQLException {
    updateBlob(findColumn(columnName), inputStream);
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateClob(int, Reader, long)");
  }

  public void updateClob(String columnName, Reader reader, long length) throws SQLException {
    updateClob(findColumn(columnName), reader, length);
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "updateClob(int, Reader)");
  }

  public void updateClob(String columnName, Reader reader) throws SQLException {
    updateClob(findColumn(columnName), reader);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getSQLXML columnIndex: {0}", columnIndex);
    String data = getString(columnIndex);
    if (data == null) {
      return null;
    }

    return new RedshiftSQLXML(connection, data);
  }

  public SQLXML getSQLXML(String columnName) throws SQLException {
    return getSQLXML(findColumn(columnName));
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    updateValue(columnIndex, xmlObject);
  }

  public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
    updateSQLXML(findColumn(columnName), xmlObject);
  }

  public String getNString(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getNString columnIndex: {0}", columnIndex);
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getNString(int)");
  }

  public String getNString(String columnName) throws SQLException {
    return getNString(findColumn(columnName));
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    if (RedshiftLogger.isEnable())
    	connection.getLogger().log(LogLevel.DEBUG, "  getNCharacterStream columnIndex: {0}", columnIndex);
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getNCharacterStream(int)");
  }

  public Reader getNCharacterStream(String columnName) throws SQLException {
    return getNCharacterStream(findColumn(columnName));
  }

  public void updateNCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader, int)");
  }

  public void updateNCharacterStream(String columnName, Reader x, int length) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x, length);
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader)");
  }

  public void updateNCharacterStream(String columnName, Reader x) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x);
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateNCharacterStream(int, Reader, long)");
  }

  public void updateNCharacterStream(String columnName, Reader x, long length) throws SQLException {
    updateNCharacterStream(findColumn(columnName), x, length);
  }

  public void updateCharacterStream(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateCharaceterStream(int, Reader, long)");
  }

  public void updateCharacterStream(String columnName, Reader reader, long length)
      throws SQLException {
    updateCharacterStream(findColumn(columnName), reader, length);
  }

  public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateCharaceterStream(int, Reader)");
  }

  public void updateCharacterStream(String columnName, Reader reader) throws SQLException {
    updateCharacterStream(findColumn(columnName), reader);
  }

  public void updateBinaryStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateBinaryStream(int, InputStream, long)");
  }

  public void updateBinaryStream(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateBinaryStream(findColumn(columnName), inputStream, length);
  }

  public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateBinaryStream(int, InputStream)");
  }

  public void updateBinaryStream(String columnName, InputStream inputStream) throws SQLException {
    updateBinaryStream(findColumn(columnName), inputStream);
  }

  public void updateAsciiStream(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateAsciiStream(int, InputStream, long)");
  }

  public void updateAsciiStream(String columnName, InputStream inputStream, long length)
      throws SQLException {
    updateAsciiStream(findColumn(columnName), inputStream, length);
  }

  public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(),
        "updateAsciiStream(int, InputStream)");
  }

  public void updateAsciiStream(String columnName, InputStream inputStream) throws SQLException {
    updateAsciiStream(findColumn(columnName), inputStream);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  private Calendar getDefaultCalendar() {
    TimestampUtils timestampUtils = connection.getTimestampUtils();
    if (timestampUtils.hasFastDefaultTimeZone()) {
      return timestampUtils.getSharedCalendar(null);
    }
    Calendar sharedCalendar = timestampUtils.getSharedCalendar(defaultTimeZone);
    if (defaultTimeZone == null) {
      defaultTimeZone = sharedCalendar.getTimeZone();
    }
    return sharedCalendar;
  }
}
