/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.util.CanEstimateSize;

/**
 * This is an internal class to hold field metadata info like table name, column name, etc.
 * This class is not meant to be used outside of pgjdbc.
 */
public class FieldMetadata implements CanEstimateSize {
  public static class Key {
    final int tableOid;
    final int positionInTable;

    Key(int tableOid, int positionInTable) {
      this.positionInTable = positionInTable;
      this.tableOid = tableOid;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key key = (Key) o;

      if (tableOid != key.tableOid) {
        return false;
      }
      return positionInTable == key.positionInTable;
    }

    @Override
    public int hashCode() {
      int result = tableOid;
      result = 31 * result + positionInTable;
      return result;
    }

    @Override
    public String toString() {
      return "Key{"
          + "tableOid=" + tableOid
          + ", positionInTable=" + positionInTable
          + '}';
    }
  }

  final String columnName;
  final String tableName;
  final String schemaName;
  final int nullable;
  final boolean autoIncrement;
  
  final String catalogName;
  final boolean readOnly;
  final boolean searchable;
  final boolean caseSensitive;

  public FieldMetadata(String columnName) {
    this(columnName, "", "", RedshiftResultSetMetaDataImpl.columnNullableUnknown, false);
  }

  public FieldMetadata(String columnName, String tableName, String schemaName, int nullable,
      boolean autoIncrement) {
    this(columnName, tableName, schemaName, nullable, autoIncrement,
  			"", false, true, false);
  }
  public FieldMetadata(String columnName, String tableName, String schemaName, int nullable,
      boolean autoIncrement, String catalogName, 
      boolean readOnly, boolean searchable,
      boolean caseSensitive) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.nullable = nullable;
    this.autoIncrement = autoIncrement;
    
    this.catalogName = catalogName;
    this.readOnly = readOnly;
    this.searchable = searchable;
    this.caseSensitive = caseSensitive;
  }

  public long getSize() {
    return columnName.length() * 2
        + tableName.length() * 2
        + schemaName.length() * 2
        + 4L
        + 1L
        + catalogName.length() * 2
        + 1L
        + 1L;
  }

  @Override
  public String toString() {
    return "FieldMetadata{"
        + "columnName='" + columnName + '\''
        + ", tableName='" + tableName + '\''
        + ", schemaName='" + schemaName + '\''
        + ", nullable=" + nullable
        + ", autoIncrement=" + autoIncrement
        + ", catalogName='" + catalogName + '\''
        + ", readOnly=" + readOnly
        + ", searchable=" + searchable
        + ", caseSensitive=" + caseSensitive
        + '}';
  }
}
