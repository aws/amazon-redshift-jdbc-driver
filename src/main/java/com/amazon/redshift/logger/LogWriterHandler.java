package com.amazon.redshift.logger;

import java.io.IOException;
import java.io.Writer;

public class LogWriterHandler implements LogHandler {
	
  private final Writer writer;

  public LogWriterHandler(Writer inWriter) throws Exception {
  	writer = inWriter;
  }
  
  @Override
  public synchronized void write(String message) throws Exception
  {
      writer.write(message);
      writer.flush();
  }
  
  @Override
  public synchronized void close() throws Exception {
  	// Do nothing as Writer is not created by the JDBC driver.
  }
  
  @Override
  public synchronized void flush() {
  	if (writer != null) {
  		try {
				writer.flush();
			} catch (IOException e) {
				// Ignore
			}
  	}
  }
  
}
