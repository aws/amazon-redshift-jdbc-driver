/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.core;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;

/**
 * Class representing a row in a {@link java.sql.ResultSet}.
 */
public class Tuple {
  private final boolean forUpdate;
  final byte[][] data;
  private final int rowSize;

  /**
   * Construct an empty tuple. Used in updatable result sets.
   * @param length the number of fields in the tuple.
   */
  public Tuple(int length) {
    this(new byte[length][], true);
  }

  /**
   * Construct a populated tuple. Used when returning results.
   * @param data the tuple data
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
    this.forUpdate = forUpdate;
    this.rowSize = rowSize;
  }
  
  /**
   * Number of fields in the tuple
   * @return number of fields
   */
  public int fieldCount() {
    return data.length;
  }

  /**
   * Total length in bytes of the tuple data.
   * @return the number of bytes in this tuple
   */
  public int length() {
  	if (rowSize != 0)
  		return rowSize;
  	else {
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
   * Total size in bytes (including overheads) of this Tuple instance on the heap (estimated)
   * @return the estimated number of bytes of heap memory used by this tuple.
   */
  public int getTupleSize() {
    int rawSize = 0;
    int nullFieldCount = 0;

    for (byte[] field : data) {
      if (field != null) {
        rawSize += field.length; // Adding raw data size
      } else {
        nullFieldCount++; // Count of null fields
      }
    }

    int refSize = RedshiftConnectionImpl.IS_64_BIT_JVM ? 8 : 4;
    int arrayHeaderSize = RedshiftConnectionImpl.IS_64_BIT_JVM ? 24 : 16;

    int overhead = (RedshiftConnectionImpl.IS_64_BIT_JVM ? 16 : 8) // Tuple object header overhead
            + refSize // Reference to the data array
            + refSize * data.length // Reference to each byte[] array
            + arrayHeaderSize * (data.length - nullFieldCount) // Overhead for each non-null byte[] array header
            + arrayHeaderSize // Overhead for the data array header
            + 5; // (4 + 1) => 4 byte for rowSize (int) and 1 byte for forUpdate (boolean)

    return rawSize + overhead;
  }

  /**
   * Get the data for the given field
   * @param index 0-based field position in the tuple
   * @return byte array of the data
   */
  public byte[] get(int index) {
    return data[index];
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
    byte[][] dataCopy = new byte[data.length][];
    System.arraycopy(data, 0, dataCopy, 0, data.length);
    return new Tuple(dataCopy, forUpdate);
  }

  /**
   * Set the given field to the given data.
   * @param index 0-based field position
   * @param fieldData the data to set
   */
  public void set(int index, byte[] fieldData) {
    if (!forUpdate) {
      throw new IllegalArgumentException("Attempted to write to readonly tuple");
    }
    data[index] = fieldData;
  }
}
