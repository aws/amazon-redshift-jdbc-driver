package com.amazon.redshift.core.v3;

import java.sql.SQLException;
// import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

public class RedshiftRowsBlockingQueue<E> extends LinkedBlockingQueue<E> {
	private static final long serialVersionUID = -7903933977591709194L;

	private int fetchSize;
	private long fetchRingBufferSizeCapacity;
	private boolean limitByBufSize;
	private AtomicLong totalFetchRingBufferSize;
	
  /** Lock held by put, offer, etc */
  private final ReentrantLock putLock = new ReentrantLock();

  /** Wait queue for waiting puts */
  private final Condition notFull = putLock.newCondition();
  
  private boolean closed = false;
  private boolean endOfResultAdded = false;
  private SQLException handlerException = null;
  private boolean skipRows = false;
  private int currentRow = -1;
  
  // This can be null for default constructor.
  private RedshiftLogger logger;  
  
  private Portal currentSuspendedPortal;

  private final double MEMORY_ESTIMATE_SCALING_FACTOR = 1.2;
	
  public RedshiftRowsBlockingQueue(int capacity) {
		super(capacity);
  	init(capacity, 0, null);
	}
  
  public RedshiftRowsBlockingQueue(int fetchSize, long fetchRingBufferSize, RedshiftLogger logger) {
  	super(
  			(fetchSize != 0 
  				 && fetchRingBufferSize == 0) 
  			? fetchSize
  			: Integer.MAX_VALUE);
  	init(fetchSize, fetchRingBufferSize, logger);
  }
  
  private void init(int fetchSize, long fetchRingBufferSize, RedshiftLogger logger) {
  	this.fetchSize = fetchSize;
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
		if (skipRows) return;
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
						Tuple row = (Tuple)e;
						long currentBufSize;

						if (RedshiftLogger.isEnable()
								&& logger != null) {
							logger.log(LogLevel.DEBUG, "put(): Buffer full. Waiting for application to read rows and make space");
						}

						// Wait buffer at capacity
						while (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity) {
							if(skipRows) {
								return;
							}
							notFull.await(1, TimeUnit.SECONDS);
						}

						if (RedshiftLogger.isEnable() && logger != null)
							logger.log(LogLevel.DEBUG, "put(): Buffer state change from full to having some space. Now adding a new row.");

						super.put(e);

						currentBufSize = totalFetchRingBufferSize.addAndGet(getNodeSize(row));

						if (currentBufSize < fetchRingBufferSizeCapacity)
							notFull.signal();
					} finally {
						putLock.unlock();
					}
				}
				else {
					super.put(e);
					totalFetchRingBufferSize.addAndGet(getNodeSize((Tuple)e));
				}
			}
		} // By size
		else
			super.put(e);
	}
  
  @Override
  public E take() throws InterruptedException {
  	currentRow++;
  	E e = super.take();
  	if (limitByBufSize) {
  		// Reduce the total buf size
  		Tuple row = (Tuple)e;
  		long currentBufSize;
  		boolean bufWasFull = (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity);  		
  		currentBufSize = totalFetchRingBufferSize.addAndGet(-getNodeSize(row));
  		
  		// Signal the waiters
  		if (bufWasFull) {
	      if (currentBufSize < fetchRingBufferSizeCapacity)
	        signalNotFull();
  		}
  	}
  	
  	return e;
  }
  
  public int getCurrentRowIndex(){
  	return currentRow;
  }
  
  public boolean endOfResult() {
  	return endOfResultAdded;
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
   */
  public void close() {
  	closed = true;
  	super.clear();
  	try {
  		// This will unblock the row reader, if row produce
  		// goes away before end of result.
			addEndOfRowsIndicator();
			closeSuspendedPortal();
		} catch (InterruptedException e) {
			// Ignore
		}
  	totalFetchRingBufferSize.set(0);
  }
  
  public void setSkipRows(){
  	skipRows = true;
  }

  /**
   * Add end-of-rows indicator
   * 
   * @throws InterruptedException throws when the thread gets interrupted.
   */
  public void addEndOfRowsIndicator() throws InterruptedException {
  	put((E)new Tuple(0));
  }

  /**
   * Add end-of-rows indicator, if not added.
   * 
   * @throws InterruptedException throws when the thread gets interrupted.
   */
  public void checkAndAddEndOfRowsIndicator() throws InterruptedException {
  	if (!endOfResultAdded) {
  		addEndOfRowsIndicator();
  		endOfResultAdded = true;  		
  	}
  }
  
  public void checkAndAddEndOfRowsIndicator(Portal currentSuspendedPortal) throws InterruptedException {
  	this.currentSuspendedPortal = currentSuspendedPortal;
  	checkAndAddEndOfRowsIndicator();
  }
  
  public Portal getSuspendedPortal() {
  	return currentSuspendedPortal;
  }

  public boolean isSuspendedPortal() {
  	return  (currentSuspendedPortal != null);
  }

  public void closeSuspendedPortal() {
    if (currentSuspendedPortal != null) {
      currentSuspendedPortal.close();
    }
  }
  
  /**
   * Signals a waiting put. Called only from take/poll.
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
   * Returns the size in bytes of an individual node of Ring buffer queue/linked list
   */
  private int getNodeSize(Tuple row) {
	  /**
	   * Node overheads are 32 bytes for 64-bit JVM and 16 bytes for 32-bit JVM
	   * For 64-bit JVM: (8 + 8 + 16) => 8 byte reference for Tuple object + 8 byte reference for next
	   *                 + 16 byte Node object header overhead
	   * Each of these are reduced to half in case of 32-bit JVM.
	   */
	  int estimatedNodeSize = row.getTupleSize() + (RedshiftConnectionImpl.IS_64_BIT_JVM ? 32 : 16);
	  return (int) (estimatedNodeSize * MEMORY_ESTIMATE_SCALING_FACTOR); // using a scaling factor for avoiding OOM errors
  }
}
