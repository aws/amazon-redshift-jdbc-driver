/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc2;

import com.amazon.redshift.core.ByteBufferSubsequence;

/**
 * Implement this interface and register the its instance to ArrayAssistantRegistry, to let Redshift
 * driver to support more array type.
 *
 * @author Minglei Tu
 */
public interface ArrayAssistant {
  /**
   * get array base type.
   *
   * @return array base type
   */
  Class<?> baseType();

  /**
   * build a array element from its binary bytes.
   *
   * @param bytes input bytes
   * @param pos position in input array
   * @param len length of the element
   * @return array element from its binary bytes
   */
  Object buildElement(byte[] bytes, int pos, int len);

  /**
   * build a array element from its binary bytes.
   *
   * @param bbs the Byte Buffer Subsequence pointing to the input bytes
   * @param pos position in input array
   * @param len length of the element
   * @return array element from its binary bytes
   */
  Object buildElement(ByteBufferSubsequence bbs, int pos, int len);

  /**
   * build an array element from its literal string.
   *
   * @param literal string representation of array element
   * @return array element
   */
  Object buildElement(String literal);
}
