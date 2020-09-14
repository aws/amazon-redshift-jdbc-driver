package com.amazon.redshift.logger;

public interface LogHandler {
	
	/**
	 * Write the message using this handler.
	 * This can be a file or console.
	 * 
	 * @param message Log entry
	 * @throws Exception throws when any error happens during write operation.
	 */
  public void write(String message) throws Exception;
  
  public void close() throws Exception;
  
  public void flush();
}
