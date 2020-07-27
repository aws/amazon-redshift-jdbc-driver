package com.amazon.redshift.logger;

import java.io.PrintWriter;

public class LogConsoleHandler implements LogHandler {
	
  private final PrintWriter writer = new PrintWriter(System.out);

  @Override
  public synchronized void write(String message) throws Exception
  {
      writer.println(message);
      writer.flush();
  }
  
  @Override
  public synchronized void close() throws Exception {
  	// Do nothing as Writer is on the stdout.
  }
  
  @Override
  public synchronized void flush() {
  	if (writer != null) {
  		writer.flush();
  	}
  }
}
