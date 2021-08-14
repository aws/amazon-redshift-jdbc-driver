package com.amazon.redshift.core.v3;

import com.amazon.redshift.core.Tuple;

import java.nio.ByteBuffer;

/**
 * Keep the state of the message loop for Ring Buffer to work on separate thread.
 * This is use in processResult(). It store all local vars of processResult() methods,
 * so it can process in multiple threads.
 * 
 * @author igarish
 *
 */
public class MessageLoopState 
{
	// All vars are package-private, so no need to expose accessor methods.
  RedshiftByteBufferBlockingQueue<ByteBuffer> bufferQueue;
  RedshiftRowsBlockingQueue<Tuple> queueTuples;
  RedshiftByteBufferBlockingQueue<ByteBuffer> queuePages;

  // At the end of a command execution we have the CommandComplete
  // message to tell us we're done, but with a describeOnly command
  // we have no real flag to let us know we're done. We've got to
  // look for the next RowDescription or NoData message and return
  // from there.
  boolean doneAfterRowDescNoData;
  boolean doneProcessingRows;
  boolean firstRow;
  
  // Constructor
  public MessageLoopState()
  {
  	initMessageLoopState(null, null, null, false);
  }
  
  public MessageLoopState(RedshiftByteBufferBlockingQueue<ByteBuffer> bufferQueue,
                          RedshiftRowsBlockingQueue<Tuple> queueTuples,
                          RedshiftByteBufferBlockingQueue<ByteBuffer> queuePages,
                          boolean doneAfterRowDescNoData)
  {
  	initMessageLoopState(bufferQueue,
                         queueTuples,
                         queuePages,
  											 doneAfterRowDescNoData);
  }

  /**
   * Initialize the object before starting the run.
   * 
   */
	void initMessageLoopState(RedshiftByteBufferBlockingQueue<ByteBuffer> bufferQueue,
                            RedshiftRowsBlockingQueue<Tuple> queueTuples,
                            RedshiftByteBufferBlockingQueue<ByteBuffer> queuePages,
                            boolean doneAfterRowDescNoData)
  {
    this.bufferQueue = bufferQueue;
    this.queueTuples = queueTuples;
		this.queuePages = queuePages;
		this.doneAfterRowDescNoData = doneAfterRowDescNoData;
		this.doneProcessingRows = false;
		this.firstRow = false;
  }
}

