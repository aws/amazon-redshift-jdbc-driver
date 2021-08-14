/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.core.ByteBufferSubsequence;
import java.nio.ByteBuffer;

/**
 * Class representing a row in a {@link java.sql.ResultSet}.
 */
public class Tuple {
  private final boolean forUpdate;
  final byte[][] data;
  final ByteBuffer page;
  final Integer offset;
  final Integer numFields;
  final int[] columnOffsets;
  private final int rowSize;
  final boolean isLastRowOnPage;

  /**
   * Construct an empty tuple. Used in updatable result sets.
   * @param length the number of fields in the tuple.
   */
  public Tuple(int length) {
    this(new byte[length][], true);
  }

  /**
   * Construct a populated tuple. Used when returning results.
   * @param data the tuple data. If this is set, page is null.
   */
  public Tuple(byte[][] data) {
    this(data, false);
  }

  public Tuple(byte[][] data, int rowSize) {
  	this(data, false, rowSize);
  }
  
  private Tuple(byte[][] data, boolean forUpdate) {
  	this(data, forUpdate, 0);
  }
 
  private Tuple(byte[][] data, boolean forUpdate, int rowSize) {
    this.data = data;
    this.page = null;
    this.offset = null;
    this.numFields = null;
    this.columnOffsets = null;
    this.forUpdate = forUpdate;
    this.rowSize = rowSize;
    this.isLastRowOnPage = false;
  }

  /**
   * Construct a tuple using row page offsets. Used when returning results.
   * @param page the buffer data this tuple points to. If this is set, data is null.
   * @param offset the offset within the page the row is on
   * @param numFields the number of fields a.k.a. columns this row has
   * @param columnOffsets the offsets within the page where each column starts for this row
   * @param rowSize the total amount of bytes this row has
   * @param isLastRowOnPage denotes if this row is the last one in the page
   */

  public Tuple(ByteBuffer page, int offset, int numFields, int[] columnOffsets, int rowSize, boolean isLastRowOnPage) {
    this.data = null;
    this.page = page;
    this.offset = offset;
    this.numFields = numFields;
    this.columnOffsets = columnOffsets;
    this.forUpdate = false;
    this.rowSize = rowSize;
    this.isLastRowOnPage = isLastRowOnPage;
  }
  
  /**
   * Number of fields in the tuple
   * @return number of fields
   */
  public int fieldCount() {
    if (data != null) {
      return data.length;
    } else {
      return numFields;
    }
  }

  /**
   * Total length in bytes of the tuple data.
   * @return the number of bytes in this tuple
   */
  public int length() {
  	if (data == null || rowSize != 0) {
      return rowSize;
    } else {
	    int length = 0;
	    for (byte[] field : data) {
	      if (field != null) {
	        length += field.length;
	      }
	    }
	    return length;
  	}
  }

  /**
   * Gets the page this row is in
   * @return the page this Tuple points to
   */
  public ByteBuffer getPage() {
    return page;
  }

  /**
   * Determines whether this row is the last row on the page. Always false if data is not null.
   * @return true if this Tuple is the last row on the page
   */
  public boolean isLastRowOnPage() {
    return isLastRowOnPage;
  }

  public boolean isNull(int index) {
    if (data != null) {
      return data[index] == null;
    } else {
      int columnOffset = columnOffsets[index];
      int columnSize = page.getInt(columnOffset);
      return columnSize < 0;
    }
  }

  /**
   * Get the data for the given field
   * @param index 0-based field position in the tuple
   * @return byte array of the data
   */
  public byte[] get(int index) {
    if (data != null) {
      return data[index];
    } else {
      int columnOffset = columnOffsets[index];
      int columnSize = page.getInt(columnOffset);
      if (columnSize < 0) {
        return null;
      }
      byte[] result = new byte[columnSize];
      for (int i = 0; i < columnSize; i++) {
        result[i] = page.get(columnOffset+4+i);
      }
      return result;
    }
  }

  /**
   * Gets the ByteBuffer object this Tuple points to with the column offset and column size of the column indicated by
   * the passed index.
   * @param index the index of the column in this Tuple we are returning
   * @return ByteBufferSequence tuple object that contains the ByteBuffer page, the column offset, and the column size
   *         of the column we are returning
   */
  public ByteBufferSubsequence getByteBufferSubsequence(int index) {
    if (data != null) {
      return new ByteBufferSubsequence(data[index], 0, data[index].length);
    } else {
      int columnOffset = columnOffsets[index];
      int columnSize = page.getInt(columnOffset);
      return new ByteBufferSubsequence(page.array(), columnOffset + 4, columnSize);
    }
  }

  /**
   * Create a copy of the tuple for updating.
   * @return a copy of the tuple that allows updates
   */
  public Tuple updateableCopy() {
    return copy(true);
  }

  /**
   * Create a read-only copy of the tuple
   * @return a copy of the tuple that does not allow updates
   */
  public Tuple readOnlyCopy() {
    return copy(false);
  }

  private Tuple copy(boolean forUpdate) {
    byte[][] dataCopy;
    if (data != null) {
      dataCopy = new byte[data.length][];
      System.arraycopy(data, 0, dataCopy, 0, data.length);
    } else {
      dataCopy = new byte[numFields][];
      for (int i = 0; i < numFields; i++) {
        dataCopy[i] = get(i);
      }
    }
    return new Tuple(dataCopy, forUpdate);
  }

  /**
   * Set the given field to the given data.
   * @param index 0-based field position
   * @param fieldData the data to set
   */
  public void set(int index, byte[] fieldData) {
    // is it possible to call this method on a Tuple with page instead of the standard data?
    // If so, we need to think about being able to update the ByteBuffer page and control it's capacity and copy over data
    if (!forUpdate || data == null) {
      throw new IllegalArgumentException("Attempted to write to readonly tuple");
    }
    data[index] = fieldData;
  }
}
