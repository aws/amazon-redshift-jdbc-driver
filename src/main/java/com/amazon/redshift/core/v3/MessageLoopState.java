package com.amazon.redshift.core.v3;

import com.amazon.redshift.core.Tuple;

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
	RedshiftRowsBlockingQueue<Tuple> queueTuples;

  // At the end of a command execution we have the CommandComplete
  // message to tell us we're done, but with a describeOnly command
  // we have no real flag to let us know we're done. We've got to
  // look for the next RowDescription or NoData message and return
  // from there.
  boolean doneAfterRowDescNoData;
  
  // Constructor
  public MessageLoopState()
  {
  	initMessageLoopState(null, false);
  }
  
  public MessageLoopState(RedshiftRowsBlockingQueue<Tuple> queueTuples,
													boolean doneAfterRowDescNoData)
  {
  	initMessageLoopState(queueTuples, 
  											doneAfterRowDescNoData);
  }

  /**
   * Initialize the object before starting the run.
   * 
   */
	void initMessageLoopState(RedshiftRowsBlockingQueue<Tuple> queueTuples,
														boolean doneAfterRowDescNoData)
  {
		this.queueTuples = queueTuples;
		this.doneAfterRowDescNoData = doneAfterRowDescNoData;
  }
}

