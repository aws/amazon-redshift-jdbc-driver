package com.amazon.redshift.core.v3;

import java.sql.SQLException;
// import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.amazon.redshift.core.Tuple;

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
	
  public RedshiftRowsBlockingQueue(int capacity) {
		super(capacity);
  	init(capacity, 0);
	}
  
  public RedshiftRowsBlockingQueue(int fetchSize, long fetchRingBufferSize) {
  	super(
  			(fetchSize != 0 
  				 && fetchRingBufferSize == 0) 
  			? fetchSize
  			: Integer.MAX_VALUE);
  	init(fetchSize, fetchRingBufferSize);
  }
  
  private void init(int fetchSize, long fetchRingBufferSize) {
  	this.fetchSize = fetchSize;
  	this.fetchRingBufferSizeCapacity = fetchRingBufferSize;
  	limitByBufSize =  (fetchRingBufferSize != 0);
		totalFetchRingBufferSize = new AtomicLong();
  }
  
  @Override
  public void put(E e) throws InterruptedException {
  	if (skipRows) return;
  	if (limitByBufSize) {
  		if (e != null) {
  			// Is buffer at full capacity?
        if(totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity) {
    			final ReentrantLock putLock = this.putLock;
          
	        putLock.lockInterruptibly();
	        try {
			  		Tuple row = (Tuple)e;
			  		long currentBufSize;
			  		
		  			// Wait buffer at capacity
	          while (totalFetchRingBufferSize.get() >= fetchRingBufferSizeCapacity) {
	            notFull.await(1, TimeUnit.SECONDS);
	          }
	            
	    	  	super.put(e);
			  			
	    	  	currentBufSize = totalFetchRingBufferSize.addAndGet(row.length());
	          
	          if (currentBufSize < fetchRingBufferSizeCapacity)
	            notFull.signal();
	        } finally {
	          putLock.unlock();
	        }
        }
        else
    	  	super.put(e);
  		}
  	}
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
  		currentBufSize = totalFetchRingBufferSize.addAndGet(-row.length());
  		
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
		} catch (InterruptedException e) {
			// Ignore
		}
  	totalFetchRingBufferSize.set(0);
  }
  
  public void setSkipRows(){
  	skipRows = true;
  }

  /** Add end-of-rows indicator
   * 
   */
  public void addEndOfRowsIndicator() throws InterruptedException {
  	put((E)new Tuple(0));
  }

  /** Add end-of-rows indicator, if not added.
   * 
   */
  public void checkAndAddEndOfRowsIndicator() throws InterruptedException {
  	if (!endOfResultAdded) {
  		addEndOfRowsIndicator();
  		endOfResultAdded = true;  		
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
}
