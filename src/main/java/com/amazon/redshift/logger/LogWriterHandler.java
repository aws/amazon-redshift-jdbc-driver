package com.amazon.redshift.logger;

import java.io.IOException;
import java.io.Writer;

import com.amazon.redshift.jdbc.ResourceLock;

public class LogWriterHandler implements LogHandler {

	private final Writer writer;
	private final ResourceLock lock = new ResourceLock();

	public LogWriterHandler(Writer inWriter) throws Exception {
		writer = inWriter;
	}

	@Override
	public void write(String message) throws Exception {
		try (ResourceLock ignore = lock.obtain()) {
			writer.write(message);
			writer.flush();
		}
	}

	@Override
	public void close() throws Exception {
		try (ResourceLock ignore = lock.obtain()) {
			// Do nothing as Writer is not created by the JDBC driver.
		}
	}

	@Override
	public void flush() {
		try (ResourceLock ignore = lock.obtain()) {
			if (writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					// Ignore
				}
			}
		}
	}

}
