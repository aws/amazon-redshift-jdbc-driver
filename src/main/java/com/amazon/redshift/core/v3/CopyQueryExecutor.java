package com.amazon.redshift.core.v3;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.redshift.copy.CopyIn;
import com.amazon.redshift.copy.CopyOperation;
import com.amazon.redshift.copy.CopyOut;
import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.ByteStreamWriter;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

//
// Copy subprotocol implementation
//

class CopyQueryExecutor {
	
	private QueryExecutorImpl queryExecutor;
  RedshiftLogger logger;
  final RedshiftStream pgStream;
	
	
	CopyQueryExecutor(QueryExecutorImpl queryExecutor,
										RedshiftLogger logger,
										RedshiftStream pgStream) {
		this.queryExecutor = queryExecutor;
		this.logger = logger;
		this.pgStream = pgStream;
	}
	
  /**
   * Sends given query to BE to start, initialize and lock connection for a CopyOperation.
   *
   * @param sql COPY FROM STDIN / COPY TO STDOUT statement
   * @return CopyIn or CopyOut operation object
   * @throws SQLException on failure
   */
  CopyOperation startCopy(String sql, boolean suppressBegin)
      throws SQLException {
  	
    // Wait for current ring buffer thread to finish, if any.
  	// Shouldn't call from synchronized method, which can cause dead-lock.
  	queryExecutor.waitForRingBufferThreadToFinish(false, false, false, null, null);
  	
    synchronized(queryExecutor) {
    	queryExecutor.waitOnLock();
	    
	    if (!suppressBegin) {
	    	queryExecutor.doSubprotocolBegin();
	    }
	    byte[] buf = Utils.encodeUTF8(sql);
	
	    try {
	    	if(RedshiftLogger.isEnable())    	
	    		logger.log(LogLevel.DEBUG, " FE=> Query(CopyStart)");
	
	      pgStream.sendChar('Q');
	      pgStream.sendInteger4(buf.length + 4 + 1);
	      pgStream.send(buf);
	      pgStream.sendChar(0);
	      pgStream.flush();
	
	      return processCopyResults(null, true);
	      // expect a CopyInResponse or CopyOutResponse to our query above
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when starting copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
    }
  }

  void cancelCopy(CopyOperationImpl op) throws SQLException {
    if (!queryExecutor.hasLock(op)) {
      throw new RedshiftException(GT.tr("Tried to cancel an inactive copy operation"),
          RedshiftState.OBJECT_NOT_IN_STATE);
    }

    SQLException error = null;
    int errors = 0;

    try {
      if (op instanceof CopyIn) {
        synchronized (queryExecutor) {
        	if(RedshiftLogger.isEnable())    	
        		logger.log(LogLevel.DEBUG, "FE => CopyFail");
          final byte[] msg = Utils.encodeUTF8("Copy cancel requested");
          pgStream.sendChar('f'); // CopyFail
          pgStream.sendInteger4(5 + msg.length);
          pgStream.send(msg);
          pgStream.sendChar(0);
          pgStream.flush();
          do {
            try {
              processCopyResults(op, true); // discard rest of input
            } catch (SQLException se) { // expected error response to failing copy
              errors++;
              if (error != null) {
                SQLException e = se;
                SQLException next;
                while ((next = e.getNextException()) != null) {
                  e = next;
                }
                e.setNextException(error);
              }
              error = se;
            }
          } while (queryExecutor.hasLock(op));
        }
      } else if (op instanceof CopyOut) {
      	queryExecutor.sendQueryCancel();
      }

    } catch (IOException ioe) {
      throw new RedshiftException(GT.tr("Database connection failed when canceling copy operation"),
          RedshiftState.CONNECTION_FAILURE, ioe);
    } finally {
      // Need to ensure the lock isn't held anymore, or else
      // future operations, rather than failing due to the
      // broken connection, will simply hang waiting for this
      // lock.
      synchronized (queryExecutor) {
        if (queryExecutor.hasLock(op)) {
        	queryExecutor.unlock(op);
        }
      }
    }

    if (op instanceof CopyIn) {
      if (errors < 1) {
        throw new RedshiftException(GT.tr("Missing expected error response to copy cancel request"),
            RedshiftState.COMMUNICATION_ERROR);
      } else if (errors > 1) {
        throw new RedshiftException(
            GT.tr("Got {0} error responses to single copy cancel request", String.valueOf(errors)),
            RedshiftState.COMMUNICATION_ERROR, error);
      }
    }
  }
  
  AtomicBoolean processingCopyResults = new AtomicBoolean(false);
  
  /**
   * Handles copy sub protocol responses from server. Unlocks at end of sub protocol, so operations
   * on pgStream or QueryExecutor are not allowed in a method after calling this!
   *
   * @param block whether to block waiting for input
   * @return CopyIn when COPY FROM STDIN starts; CopyOut when COPY TO STDOUT starts; null when copy
   *         ends; otherwise, the operation given as parameter.
   * @throws SQLException in case of misuse
   * @throws IOException from the underlying connection
   */
  CopyOperationImpl processCopyResults(CopyOperationImpl op, boolean block)
      throws SQLException, IOException {

    /*
    * fixes issue #1592 where one thread closes the stream and another is reading it
     */
    if (pgStream.isClosed()) {
      throw new RedshiftException(GT.tr("RedshiftStream is closed",
        op.getClass().getName()), RedshiftState.CONNECTION_DOES_NOT_EXIST);
    }
    /*
    *  This is a hack as we should not end up here, but sometimes do with large copy operations.
     */
    if ( processingCopyResults.compareAndSet(false,true) == false ) {
    	if(RedshiftLogger.isEnable())    	
    		logger.log(LogLevel.INFO, "Ignoring request to process copy results, already processing");
      return null;
    }

    // put this all in a try, finally block and reset the processingCopyResults in the finally clause
    try {
      boolean endReceiving = false;
      SQLException error = null;
      SQLException errors = null;
      int len;

      while (!endReceiving && (block || pgStream.hasMessagePending())) {

        // There is a bug in the server's implementation of the copy
        // protocol. It returns command complete immediately upon
        // receiving the EOF marker in the binary protocol,
        // potentially before we've issued CopyDone. When we are not
        // blocking, we don't think we are done, so we hold off on
        // processing command complete and any subsequent messages
        // until we actually are done with the copy.
        //
        if (!block) {
          int c = pgStream.peekChar();
          if (c == 'C') {
            // CommandComplete
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CommandStatus, Ignored until CopyDone");
            break;
          }
        }

        int c = pgStream.receiveChar();
        switch (c) {

          case 'A': // Asynchronous Notify

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE Asynchronous Notification while copying");

          	queryExecutor.receiveAsyncNotify();
            break;

          case 'N': // Notice Response
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE Notification while copying");

          	queryExecutor.addWarning(queryExecutor.receiveNoticeResponse());
            break;

          case 'C': // Command Complete

            String status = queryExecutor.receiveCommandStatus();

            try {
              if (op == null) {
                throw new RedshiftException(GT
                    .tr("Received CommandComplete ''{0}'' without an active copy operation", status),
                    RedshiftState.OBJECT_NOT_IN_STATE);
              }
              op.handleCommandStatus(status);
            } catch (SQLException se) {
              error = se;
            }

            block = true;
            break;

          case 'E': // ErrorMessage (expected response to CopyFail)

            error = queryExecutor.receiveErrorResponse(false);
            // We've received the error and we now expect to receive
            // Ready for query, but we must block because it might still be
            // on the wire and not here yet.
            block = true;
            break;

          case 'G': // CopyInResponse

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CopyInResponse");

            if (op != null) {
              error = new RedshiftException(GT.tr("Got CopyInResponse from server during an active {0}",
                  op.getClass().getName()), RedshiftState.OBJECT_NOT_IN_STATE);
            }

            op = new CopyInImpl();
            initCopy(op);
            endReceiving = true;
            break;

          case 'H': // CopyOutResponse

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CopyOutResponse");

            if (op != null) {
              error = new RedshiftException(GT.tr("Got CopyOutResponse from server during an active {0}",
                  op.getClass().getName()), RedshiftState.OBJECT_NOT_IN_STATE);
            }

            op = new CopyOutImpl();
            initCopy(op);
            endReceiving = true;
            break;

          case 'W': // CopyBothResponse

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CopyBothResponse");

            if (op != null) {
              error = new RedshiftException(GT.tr("Got CopyBothResponse from server during an active {0}",
                  op.getClass().getName()), RedshiftState.OBJECT_NOT_IN_STATE);
            }

            op = new CopyDualImpl();
            initCopy(op);
            endReceiving = true;
            break;

          case 'd': // CopyData

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CopyData");

            len = pgStream.receiveInteger4() - 4;

            assert len > 0 : "Copy Data length must be greater than 4";

            byte[] buf = pgStream.receive(len);
            if (op == null) {
              error = new RedshiftException(GT.tr("Got CopyData without an active copy operation"),
                  RedshiftState.OBJECT_NOT_IN_STATE);
            } else if (!(op instanceof CopyOut)) {
              error = new RedshiftException(
                  GT.tr("Unexpected copydata from server for {0}", op.getClass().getName()),
                  RedshiftState.COMMUNICATION_ERROR);
            } else {
              op.handleCopydata(buf);
            }
            endReceiving = true;
            break;

          case 'c': // CopyDone (expected after all copydata received)

          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE CopyDone");

            len = pgStream.receiveInteger4() - 4;
            if (len > 0) {
              pgStream.receive(len); // not in specification; should never appear
            }

            if (!(op instanceof CopyOut)) {
              error = new RedshiftException("Got CopyDone while not copying from server",
                  RedshiftState.OBJECT_NOT_IN_STATE);
            }

            // keep receiving since we expect a CommandComplete
            block = true;
            break;
          case 'S': // Parameter Status
            try {
            	queryExecutor.receiveParameterStatus();
            } catch (SQLException e) {
              error = e;
              endReceiving = true;
            }
            break;

          case 'Z': // ReadyForQuery: After FE:CopyDone => BE:CommandComplete

          	queryExecutor.receiveRFQ();
            if (queryExecutor.hasLock(op)) {
            	queryExecutor.unlock(op);
            }
            op = null;
            endReceiving = true;
            break;

          // If the user sends a non-copy query, we've got to handle some additional things.
          //
          case 'T': // Row Description (response to Describe)
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE RowDescription (during copy ignored)");

          	queryExecutor.skipMessage();
            break;

          case 'D': // DataRow
          	if(RedshiftLogger.isEnable())    	
          		logger.log(LogLevel.DEBUG, " <=BE DataRow (during copy ignored)");

          	queryExecutor.skipMessage();
            break;

          default:
            throw new IOException(
                GT.tr("Unexpected packet type during copy: {0}", Integer.toString(c)));
        }

        // Collect errors into a neat chain for completeness
        if (error != null) {
          if (errors != null) {
            error.setNextException(errors);
          }
          errors = error;
          error = null;
        }
      }

      if (errors != null) {
        throw errors;
      }
      return op;

    } finally {
      /*
      reset here in the finally block to make sure it really is cleared
       */
      processingCopyResults.set(false);
    }
  }
  
  /**
   * Locks connection and calls initializer for a new CopyOperation Called via startCopy ->
   * processCopyResults.
   *
   * @param op an uninitialized CopyOperation
   * @throws SQLException on locking failure
   * @throws IOException on database connection failure
   */
  void initCopy(CopyOperationImpl op) throws SQLException, IOException {
  	synchronized(queryExecutor) {
	    pgStream.receiveInteger4(); // length not used
	    int rowFormat = pgStream.receiveChar();
	    int numFields = pgStream.receiveInteger2();
	    int[] fieldFormats = new int[numFields];
	
	    for (int i = 0; i < numFields; i++) {
	      fieldFormats[i] = pgStream.receiveInteger2();
	    }
	
	    queryExecutor.lock(op);
	    op.init(queryExecutor, rowFormat, fieldFormats);
  	}
  }
  
  /**
   * Finishes writing to copy and unlocks connection.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @return number of rows updated for server versions 8.2 or newer
   * @throws SQLException on failure
   */
  long endCopy(CopyOperationImpl op) throws SQLException {
  	synchronized(queryExecutor) {
	    if (!queryExecutor.hasLock(op)) {
	      throw new RedshiftException(GT.tr("Tried to end inactive copy"), RedshiftState.OBJECT_NOT_IN_STATE);
	    }
	
	    try {
	    	if(RedshiftLogger.isEnable())    	
	    		logger.log(LogLevel.DEBUG, " FE=> CopyDone");
	
	      pgStream.sendChar('c'); // CopyDone
	      pgStream.sendInteger4(4);
	      pgStream.flush();
	
	      do {
	        processCopyResults(op, true);
	      } while (queryExecutor.hasLock(op));
	      return op.getHandledRowCount();
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when ending copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
  	}
  }
  
  /**
   * Sends data during a live COPY IN operation. Only unlocks the connection if server suddenly
   * returns CommandComplete, which should not happen
   *
   * @param op the CopyIn operation presumably currently holding lock on this connection
   * @param data bytes to send
   * @param off index of first byte to send (usually 0)
   * @param siz number of bytes to send (usually data.length)
   * @throws SQLException on failure
   */
  void writeToCopy(CopyOperationImpl op, byte[] data, int off, int siz)
      throws SQLException {
  	synchronized(queryExecutor) {
	    if (!queryExecutor.hasLock(op)) {
	      throw new RedshiftException(GT.tr("Tried to write to an inactive copy operation"),
	          RedshiftState.OBJECT_NOT_IN_STATE);
	    }
	
	  	if(RedshiftLogger.isEnable())    	
	  		logger.log(LogLevel.DEBUG, " FE=> CopyData({0})", siz);
	
	    try {
	      pgStream.sendChar('d');
	      pgStream.sendInteger4(siz + 4);
	      pgStream.send(data, off, siz);
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when writing to copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
  	}
  }
  
  /**
   * Sends data during a live COPY IN operation. Only unlocks the connection if server suddenly
   * returns CommandComplete, which should not happen
   *
   * @param op   the CopyIn operation presumably currently holding lock on this connection
   * @param from the source of bytes, e.g. a ByteBufferByteStreamWriter
   * @throws SQLException on failure
   */
  public void writeToCopy(CopyOperationImpl op, ByteStreamWriter from)
      throws SQLException {
  	synchronized(queryExecutor) {
	    if (!queryExecutor.hasLock(op)) {
	      throw new RedshiftException(GT.tr("Tried to write to an inactive copy operation"),
	          RedshiftState.OBJECT_NOT_IN_STATE);
	    }
	
	    int siz = from.getLength();
	  	if(RedshiftLogger.isEnable())    	
	  		logger.log(LogLevel.DEBUG, " FE=> CopyData({0})", siz);
	
	    try {
	      pgStream.sendChar('d');
	      pgStream.sendInteger4(siz + 4);
	      pgStream.send(from);
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when writing to copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
  	}
  }
  
  public void flushCopy(CopyOperationImpl op) throws SQLException {
  	synchronized(queryExecutor) {
	    if (!queryExecutor.hasLock(op)) {
	      throw new RedshiftException(GT.tr("Tried to write to an inactive copy operation"),
	          RedshiftState.OBJECT_NOT_IN_STATE);
	    }
	
	    try {
	      pgStream.flush();
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when writing to copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
  	}
  }
  
  /**
   * Wait for a row of data to be received from server on an active copy operation
   * Connection gets unlocked by processCopyResults() at end of operation.
   *
   * @param op the copy operation presumably currently holding lock on this connection
   * @param block whether to block waiting for input
   * @throws SQLException on any failure
   */
  void readFromCopy(CopyOperationImpl op, boolean block) throws SQLException {
  	synchronized(queryExecutor) {
	    if (!queryExecutor.hasLock(op)) {
	      throw new RedshiftException(GT.tr("Tried to read from inactive copy"),
	          RedshiftState.OBJECT_NOT_IN_STATE);
	    }
	
	    try {
	      processCopyResults(op, block); // expect a call to handleCopydata() to store the data
	    } catch (IOException ioe) {
	      throw new RedshiftException(GT.tr("Database connection failed when reading from copy"),
	          RedshiftState.CONNECTION_FAILURE, ioe);
	    }
  	}
  }
}
