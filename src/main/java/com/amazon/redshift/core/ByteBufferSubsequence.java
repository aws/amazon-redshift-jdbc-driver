/*
 * Copyright (c) 2021, Amazon LLC.
 */

package com.amazon.redshift.core;

import java.nio.ByteBuffer;

/**
 * Class for returning a portion of a ByteBuffer to help in retreiving results from
 * {@link com.amazon.redshift.core.Tuple} without copying data.
 */
public class ByteBufferSubsequence {
  // a variable pointing to the byte array linked to the ByteBuffer in memory containing the data info
  public byte[] page;
  // the index in the ByteBuffer in which the data starts
  public int index;
  // the amount of bytes to read to get the entire data
  public int length;

  public ByteBufferSubsequence(byte[] page, int index, int length) {
    this.page = page;
    this.index = index;
    this.length = length;
  }
}