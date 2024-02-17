/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.largeobject.LargeObject;

import java.sql.SQLException;

public class RedshiftBlob extends AbstractBlobClob implements java.sql.Blob {
  private final ResourceLock lock = new ResourceLock();
  public RedshiftBlob(com.amazon.redshift.core.BaseConnection conn, long oid) throws SQLException {
    super(conn, oid);
  }

  public java.io.InputStream getBinaryStream(long pos, long length)
      throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    checkFreed();
    LargeObject subLO = getLo(false).copy();
    addSubLO(subLO);
    if (pos > Integer.MAX_VALUE) {
      subLO.seek64(pos - 1, LargeObject.SEEK_SET);
    } else {
      subLO.seek((int) pos - 1, LargeObject.SEEK_SET);
    }
    return subLO.getInputStream(length);
	  }
  }

  public int setBytes(long pos, byte[] bytes) throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    return setBytes(pos, bytes, 0, bytes.length);
	  }
  }

  public int setBytes(long pos, byte[] bytes, int offset, int len)
      throws SQLException {
	  try (ResourceLock ignore = lock.obtain()) {
    assertPosition(pos);
    getLo(true).seek((int) (pos - 1));
    getLo(true).write(bytes, offset, len);
    return len;
	  }
  }
}
