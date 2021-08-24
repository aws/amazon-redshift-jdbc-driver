package com.amazon.redshift.core.v3;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

public class RedshiftByteBufferBlockingQueue<E> extends LinkedBlockingQueue<E> {
  private static final long serialVersionUID = -7903933977591709194L;

  private long fetchRingBufferSizeCapacity;
  protected boolean limitByBufSize;
  private AtomicLong totalFetchRingBufferSize;

  /** Lock held by put, offer, etc */
  private final ReentrantLock putLock = new ReentrantLock();
  /** Lock held by take, poll, remove, etc */
  private final ReentrantLock takeLock = new ReentrantLock();

  /** Wait queue for waiting puts */
  private final Condition notFull = putLock.newCondition();
  /** Wait queue for waiting takes */
  private final Condition notEmpty = takeLock.newCondition();

  private boolean closed = false;
  private SQLException handlerException = null;
  private boolean doneReadingRows = false;

  // This can be null for default constructor.
  private RedshiftLogger logger;

  public RedshiftByteBufferBlockingQueue(int capacity) {
    super(capacity);
    init(capacity, 0, null);
  }

  public RedshiftByteBufferBlockingQueue(int fetchSize, long fetchRingBufferSize, RedshiftLogger logger) {
    super(
        (fetchSize != 0
            && fetchRingBufferSize == 0)
            ? fetchSize
            : Integer.MAX_VALUE);
    init(fetchSize, fetchRingBufferSize, logger);
  }

  private void init(int fetchSize, long fetchRingBufferSize, RedshiftLogger logger) {
    this.fetchRingBufferSizeCapacity = fetchRingBufferSize;
    this.logger = logger;
    limitByBufSize =  (fetchRingBufferSize != 0);
    totalFetchRingBufferSize = new AtomicLong();

    if (RedshiftLogger.isEnable()
        && logger != null) {
      logger.log(LogLevel.DEBUG, "init(): limitByBufSize={0} , totalFetchRingBufferSize={1}, fetchRingBufferSizeCapacity = {2}, fetchSize = {3}",
          limitByBufSize, totalFetchRingBufferSize.get(), fetchRingBufferSizeCapacity, fetchSize);
    }
  }

  @Override
  public void put(E e) throws InterruptedException {
    if (limitByBufSize) {
      if (e != null) {

/*  	    if (RedshiftLogger.isEnable()
  	  			&& logger != null) {
  	    	logger.log(LogLevel.DEBUG, "put(): limitByBufSize={0} , totalFetchRingBufferSize={1}, fetchRingBufferSizeCapacity = {2}, fetchSize = {3}",
  	    															limitByBufSize, totalFetchRingBufferSize.get(), fetchRingBufferSizeCapacity, fetchSize);
  	    } */

        // Is buffer at full capacity?
        if(totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity) {
          final ReentrantLock putLock = this.putLock;

          putLock.lockInterruptibly();
          try {
            ByteBuffer byteBuffer = (ByteBuffer)e;
            long currentBufSize;

            if (RedshiftLogger.isEnable()
                && logger != null) {
              logger.log(LogLevel.DEBUG, "put(): Buffer full. Waiting for application to read bytebuffer and make space");
            }

            // Wait buffer at capacity
            while (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity) {
              notFull.await(1, TimeUnit.SECONDS);
            }

            if (RedshiftLogger.isEnable() && logger != null)
              logger.log(LogLevel.DEBUG, "put(): Buffer state change from full to having some space. Now adding a new bytebuffer.");

            super.put(e);
            signalNotEmpty();

            currentBufSize = totalFetchRingBufferSize.addAndGet(byteBuffer.limit());

            if (currentBufSize < fetchRingBufferSizeCapacity)
              notFull.signal();
          } finally {
            putLock.unlock();
          }
        }
        else {
          super.put(e);
          signalNotEmpty();
          totalFetchRingBufferSize.addAndGet(((ByteBuffer) e).limit());
        }
      }
    } // By size
    else {
      super.put(e);
      signalNotEmpty();
    }
  }

  @Override
  public E take() throws InterruptedException {
    E e;

    final ReentrantLock takeLock = this.takeLock;

    takeLock.lockInterruptibly();
    try {
      // Wait buffer is empty
      while (super.size() == 0 && !doneReadingRows) {
        notEmpty.await(1, TimeUnit.SECONDS);
      }

      if (super.size() > 0) {
        e = super.take();
      } else {
        return null;
      }

      if (super.size() > 0 || doneReadingRows) {
        notEmpty.signal();
      }
    } finally {
      takeLock.unlock();
    }

    if (limitByBufSize) {
      ByteBuffer byteBuffer = (ByteBuffer)e;
      // Reduce the total buf size
      long currentBufSize;
      boolean bufWasFull = (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity);
      currentBufSize = totalFetchRingBufferSize.addAndGet(-byteBuffer.limit());

      // Signal the waiters
      if (bufWasFull) {
        if (currentBufSize < fetchRingBufferSizeCapacity)
          signalNotFull();
      }
    }

    return e;
  }

  @Override
  public boolean remove(Object o) {
    boolean b = super.remove(o);

    if (b && limitByBufSize) {
      ByteBuffer byteBuffer = (ByteBuffer)o;
      // Reduce the total buf size
      long currentBufSize;
      boolean bufWasFull = (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity);
      currentBufSize = totalFetchRingBufferSize.addAndGet(-byteBuffer.limit());

      // Signal the waiters
      if (bufWasFull) {
        if (currentBufSize < fetchRingBufferSizeCapacity)
          signalNotFull();
      }
    }

    return b;
  }

  public void setHandlerException(SQLException ex) {
    handlerException = ex;
  }

  public SQLException getHandlerException() {
    SQLException ex = handlerException;
    handlerException = null;
    return ex;
  }

  /**
   * Close the queue.
   *
   */
  public void close() {
    closed = true;
    super.clear();
    totalFetchRingBufferSize.set(0);
  }

  /**
   * Signals a waiting put. Called only from take/poll/remove.
   */
  private void signalNotFull() {
    final ReentrantLock putLock = this.putLock;
    putLock.lock();
    try {
      notFull.signal();
    } finally {
      putLock.unlock();
    }
  }

  /**
   * Signals a waiting take. Called only from put/offer.
   */
  private void signalNotEmpty() {
    final ReentrantLock takeLock = this.takeLock;
    takeLock.lock();
    try {
      notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
  }

  public void setDoneReadingRows() {
    doneReadingRows = true;
    signalNotEmpty();
  }
}
