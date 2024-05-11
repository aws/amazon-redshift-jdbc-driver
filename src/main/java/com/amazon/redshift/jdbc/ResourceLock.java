/*
 * Copyright (c) 2005, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Extends a ReentrantLock for use in try-with-resources block.
 *
 * <h2>Example use</h2>
 * <pre>{@code
 *
 *   try (ResourceLock ignore = lock.obtain()) {
 *     // do something while holding the resource lock
 *   }
 *
 * }</pre>
 */
public final class ResourceLock extends ReentrantLock implements AutoCloseable {
  private static final long serialVersionUID = 8459051451899973878L;

  /**
   * Obtain a lock and return the ResourceLock for use in try-with-resources block.
   */
  public ResourceLock obtain() {
    lock();
    return this;
  }

  /**
   * Unlock on exit of try-with-resources block.
   */
  @Override
  public void close() {
    this.unlock();
  }
}