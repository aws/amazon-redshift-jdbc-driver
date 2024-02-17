/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.redshift.jdbc.ResourceLock;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

public class SharedTimer {
  // Incremented for each Timer created, this allows each to have a unique Timer name
  private static final AtomicInteger timerCount = new AtomicInteger(0);

  private static final RedshiftLogger logger = RedshiftLogger.getDriverLogger();
  private volatile Timer timer = null;
  private final AtomicInteger refCount = new AtomicInteger(0);
  private final ResourceLock lock = new ResourceLock();

  public SharedTimer() {
  }

  public int getRefCount() {
    return refCount.get();
  }

  public Timer getTimer() {
	try (ResourceLock ignore = lock.obtain()) {
	    if (timer == null) {
	      int index = timerCount.incrementAndGet();
	
	      /*
	       Temporarily switch contextClassLoader to the one that loaded this driver to avoid TimerThread preventing current
	       contextClassLoader - which may be the ClassLoader of a web application - from being GC:ed.
	       */
	      final ClassLoader prevContextCL = Thread.currentThread().getContextClassLoader();
	      try {
	        /*
	         Scheduled tasks whould not need to use .getContextClassLoader, so we just reset it to null
	         */
	        Thread.currentThread().setContextClassLoader(null);
	
	        timer = new Timer("Redshift-JDBC-SharedTimer-" + index, true);
	      } finally {
	        Thread.currentThread().setContextClassLoader(prevContextCL);
	      }
	    }
	    refCount.incrementAndGet();
	    return timer;
	}
  }

  public void releaseTimer() {
	try (ResourceLock ignore = lock.obtain()) {
	    int count = refCount.decrementAndGet();
	    if (count > 0) {
	      // There are outstanding references to the timer so do nothing
	    	if(RedshiftLogger.isEnable() && logger != null)
	    		logger.log(LogLevel.DEBUG, "Outstanding references still exist so not closing shared Timer");
	    } else if (count == 0) {
	      // This is the last usage of the Timer so cancel it so it's resources can be release.
	    	if(RedshiftLogger.isEnable() && logger != null)
	    		logger.log(LogLevel.DEBUG, "No outstanding references to shared Timer, will cancel and close it");
	      if (timer != null) {
	        timer.cancel();
	        timer = null;
	      }
	    } else {
	      // Should not get here under normal circumstance, probably a bug in app code.
	    	if(RedshiftLogger.isEnable() && logger != null)
		      logger.log(LogLevel.INFO,
		          "releaseTimer() called too many times; there is probably a bug in the calling code");
	      refCount.set(0);
	    }
	}
  }
}
