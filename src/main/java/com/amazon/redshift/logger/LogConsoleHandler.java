package com.amazon.redshift.logger;

import java.io.PrintWriter;

import com.amazon.redshift.jdbc.ResourceLock;

public class LogConsoleHandler implements LogHandler {
  private final ResourceLock lock = new ResourceLock();
  private final PrintWriter writer = new PrintWriter(System.out);

  @Override
  public void write(String message) throws Exception
  {
	  try (ResourceLock ignore = lock.obtain()) {
      writer.println(message);
      writer.flush();
	  }
  }
  
  @Override
  public void close() throws Exception {
	  try (ResourceLock ignore = lock.obtain()) {
  	// Do nothing as Writer is on the stdout.
	  }
  }
  
  @Override
  public void flush() {
	  try (ResourceLock ignore = lock.obtain()) {
  	if (writer != null) {
  		writer.flush();
  	}
	  }
  }
}
