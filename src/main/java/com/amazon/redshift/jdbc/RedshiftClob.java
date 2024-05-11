/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

public class RedshiftClob extends AbstractBlobClob implements java.sql.Clob {
  private final ResourceLock lock = new ResourceLock();
  public RedshiftClob(com.amazon.redshift.core.BaseConnection conn, long oid) throws java.sql.SQLException {
    super(conn, oid);
  }

  public Reader getCharacterStream(long pos, long length) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "getCharacterStream(long, long)");
	  }
  }

  public int setString(long pos, String str) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "setString(long,str)");
	  }
  }

  public int setString(long pos, String str, int offset, int len) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "setString(long,String,int,int)");
	  }
  }

  public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "setAsciiStream(long)");
	  }
  }

  public java.io.Writer setCharacterStream(long pos) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "setCharacteStream(long)");
	  }
  }

  public InputStream getAsciiStream() throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    return getBinaryStream();
	  }
  }

  public Reader getCharacterStream() throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    Charset connectionCharset = Charset.forName(conn.getEncoding().name());
    return new InputStreamReader(getBinaryStream(), connectionCharset);
	  }
  }

  public String getSubString(long i, int j) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    assertPosition(i, j);
    getLo(false).seek((int) i - 1);
    return new String(getLo(false).read(j));
	  }
  }

  /**
   * For now, this is not implemented.
   */
  public long position(String pattern, long start) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "position(String,long)");
	  }
  }

  /**
   * This should be simply passing the byte value of the pattern Blob.
   */
  public long position(Clob pattern, long start) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    throw com.amazon.redshift.Driver.notImplemented(this.getClass(), "position(Clob,start)");
	  }
  }
}
