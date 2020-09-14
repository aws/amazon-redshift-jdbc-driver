package com.amazon.redshift.logger;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.sql.DriverManager;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazon.redshift.RedshiftProperty;

/**
 * Logger for each connection or at driver.
 * 
 * @author iggarish
 *
 */
public class RedshiftLogger {
	
	// Any connection enable the logging
	private static boolean isEnable = false;
	
	private static RedshiftLogger driverLogger;
	
  private volatile LogLevel level = LogLevel.OFF;
  
  private String fileName;

  private LogHandler handler;
  
  private static AtomicInteger  connectionId = new AtomicInteger();

  public RedshiftLogger(String fileName, 
  											String logLevel, 
  											boolean driver,
  											String maxLogFileSize,
  											String maxLogFileCount) {
  	
  	if (driver) {
  		this.fileName = fileName;
  		driverLogger = this;
  	}
  	else {
  		int connId = connectionId.incrementAndGet();

  		if (fileName != null) {
        String fileExt = "";
        
        if (fileName.contains("."))
        {
            fileExt = fileName
                .substring(fileName.lastIndexOf("."), fileName.length());
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }
        
  			
  			this.fileName = fileName + "_connection_" + connId 
  												+ fileExt ;
  		}
  	}
  	
  	level = (logLevel != null )
  						? LogLevel.getLogLevel(logLevel)
  						: LogLevel.OFF;
  	
  	if (level != LogLevel.OFF) {
  		try {
	      if (DriverManager.getLogWriter() != null) {
	      	handler = new LogWriterHandler(DriverManager.getLogWriter());
	      }
	      else 
	      if (this.fileName != null) {
	      	handler = new LogFileHandler(this.fileName, driver, maxLogFileSize, maxLogFileCount);
	      }
	      else {
	  			handler = new LogConsoleHandler();
	      }
  		}
  		catch(Exception ex) {
  			handler = new LogConsoleHandler();
  		}
      
      isEnable = true;
  	}
  }
  /**
   * Check for logging enable or not.
   * 
   * @return true if any of the connection enable the logging, false otherwise.
   */
  public static boolean isEnable() {
  	return isEnable;
  }
  
  public static RedshiftLogger getDriverLogger() {
  	return driverLogger;
  }

  /** True if logger associated with the connection enable the logging.
   *  Otherwise false.
   *  
   *  One logger associated with each connection.
   *  There is a separate logger at driver level.
   *  
   * @return
   */
  private boolean isEnabled() {
  	return (level != LogLevel.OFF);
  }
  
  public LogLevel getLogLevel() {
  	return level;
  }
  
  /**
   * Determines if logging should occur based on the LogLevel.
   *
   * @param level
   *          The level of logging to attempt.
   * @param log
   *          The ILogger to try to log to.
   *
   * @return  True if logging is to be done according to LogLevel; false otherwise.
   */
  public static boolean checkLogLevel(LogLevel level, RedshiftLogger log)
  {
    return (log.isEnabled()) &&
           (level.ordinal() <= log.getLogLevel().ordinal());
  }

/*  public static boolean isLoggable(LogLevel level, RedshiftLogger log) {
  	return checkLogLevel(level, log);
  } */
  
  private static StackTraceElement getStackElementAbove(String functionName)
  {
    boolean returnNextFunction = false;

    // Look for the function above the specified one.
    for (StackTraceElement s : Thread.currentThread().getStackTrace())
    {
        if (returnNextFunction)
        {
            return s;
        }
        else if (s.getMethodName().equals(functionName))
        {
            returnNextFunction = true;
        }
    }

    // Default to just returning 3 above, which should be the caller of the caller of this
    // function.
    return Thread.currentThread().getStackTrace()[3];
  }
  
  public static String maskSecureInfoInUrl(String url) {  
		String[] tokens = {
				RedshiftProperty.PWD.getName(),
				RedshiftProperty.PASSWORD.getName(),
				RedshiftProperty.IAM_ACCESS_KEY_ID.getName(),
				RedshiftProperty.IAM_SECRET_ACCESS_KEY.getName(),
				RedshiftProperty.IAM_SESSION_TOKEN.getName()
				};
		String temp = maskSecureInfo(url, tokens, "[\\?;&]");
		
		return temp;
  }
  
  public static String maskSecureInfo(String msg, String[] tokens, String tokenizer) {
  	if(msg == null) return msg;
  	StringBuilder newMsg = new StringBuilder();
  	String[] splitMsgs = msg.split(tokenizer);
  	boolean secureInfoFound = false;
  	
  	for(String splitMsg : splitMsgs) {
  		String tokenFound = null;
			String sTemp = splitMsg.toLowerCase();
  		for(String token : tokens) {
  			String temp = token.toLowerCase();
  			if(sTemp.contains(temp)) {
  				tokenFound = token;
  				secureInfoFound = true;
  				break;
  			}
  		}
  		
  		if(tokenFound == null) {
  			newMsg.append(splitMsg).append(";");
  		}
  		else {
  			newMsg.append(tokenFound).append("=***;");
  		}
  	}
  	
  	return (secureInfoFound) ? newMsg.toString() : msg;
  }

  public static Properties maskSecureInfoInProps(Properties info) {
  	
  	if(info == null) return info;
  	boolean secureInfoFound = false;
  	
		String[] propNames = {
				RedshiftProperty.PWD.getName(),
				RedshiftProperty.PASSWORD.getName(),
				RedshiftProperty.IAM_ACCESS_KEY_ID.getName(),
				RedshiftProperty.IAM_SECRET_ACCESS_KEY.getName(),
				RedshiftProperty.IAM_SESSION_TOKEN.getName()
				};
		
		Properties temp = new Properties();
		temp.putAll(info);
		for(String propName : propNames) {
			Object oldVal = replaceIgnoreCase(temp, propName, "***");
			if(oldVal != null)
				secureInfoFound = true;
		}
		
		return (secureInfoFound) ? temp : info;
  }
  
  public static String replaceIgnoreCase(Properties info, String key, String newVal) {
    String value = info.getProperty(key);
    if (null != value) {
      info.replace(key, newVal);
      return value;
    }

    // Not matching with the actual key then
    Set<Entry<Object, Object>> s = info.entrySet();
    Iterator<Entry<Object, Object>> it = s.iterator();
    while (it.hasNext()) {
     Entry<Object, Object> entry = it.next();
     if (key.equalsIgnoreCase((String) entry.getKey())) {
     	info.replace(key, newVal);
      return (String) entry.getValue();
     }
    }
    return null;
   }  
  
  private static String[] getCallerMethodName(String logFunction)
  {
    /*
     * Stack Trace:
     * 0 - dumpThreads
     * 1 - getStackTrace
     * 2 - current method (logging)
     * 3 - calling method (the one we want to log)
     * 4 - method calling the method we want to log... etc.
     */

    // Retrieve the information necessary to log the message.
    StackTraceElement element = getStackElementAbove(logFunction);
    String[] names = new String[3];
    names[2] = element.getMethodName();

    try
    {
        // Dynamically look up the name of the class.
        Class<?> originatingClass = Class.forName(element.getClassName());
        names[1] = originatingClass.getSimpleName();

        // Get the package of the class.
        names[0] = "";
        Package originatingPackage = originatingClass.getPackage();
        if (null != originatingPackage)
        {
            names[0] = originatingPackage.getName();
        }
    }
    catch (ClassNotFoundException e)
    {
        // Failed to look up the class, just omit it.
        names[0] = "<error>";
        names[1] = element.getClassName();
    }

    if (names[2].equals("<init>"))
    {
        // <init> means the constructor, so just use the class name.
        names[2] = names[1];
    }

    return names;
  }

  public void log(LogLevel logLevel, String msg, Object... msgArgs) {
  	
  	if (!checkLogLevel(logLevel, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("log");
  	
  	logMsg(logLevel, callerNames, msg, msgArgs);
  }

  public void log(LogLevel logLevel, Throwable thrown, String msg, Object... msgArgs) {
  	
  	if (!checkLogLevel(logLevel, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("log");
  	
	  StringWriter sw = new StringWriter();
	  thrown.printStackTrace(new PrintWriter(sw));
	  String stacktrace = sw.toString();	  
	  
  	logMsg(logLevel, callerNames, msg, msgArgs);
  	logMsg(logLevel, callerNames, stacktrace);
  }
  
  public void logError(Exception error) {
  	
  	if (!checkLogLevel(LogLevel.ERROR, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("logError");
	  
	  StringWriter sw = new StringWriter();
	  error.printStackTrace(new PrintWriter(sw));
	  String stacktrace = sw.toString();	  
  	
  	logMsg(LogLevel.ERROR, callerNames, stacktrace);
  }
  
  public void logError(String msg, Object... msgArgs) {
  	
  	if (!checkLogLevel(LogLevel.ERROR, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("logError");
  	
  	logMsg(LogLevel.ERROR, callerNames, msg, msgArgs);
  }

  public void logInfo(String msg, Object... msgArgs) {
  	
  	if (!checkLogLevel(LogLevel.INFO, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("logInfo");
  	
  	logMsg(LogLevel.INFO, callerNames, msg, msgArgs);
  }

  public void logFunction(boolean entry, Object... params) {
  	
  	if (!checkLogLevel(LogLevel.FUNCTION, this))
      return;
  	
  	String msg = (entry) ? " Enter " : " Return ";

  	if (params != null) {
  		StringBuffer paramVal = new StringBuffer();
  		int paramCount = 0;

			paramVal.append(msg);
			
  		if (entry)
  			paramVal.append("(");
  		
  		for (Object param : params) {
  			if (paramCount++ != 0)
  				paramVal.append(",");

  			if(param != null) {
  				if(param.getClass().isArray()) {
  					if(param instanceof Object[])
  						paramVal.append(Arrays.toString((Object[])param));
  					else
  					if(param instanceof int[])
  						paramVal.append(Arrays.toString((int[])param));
  					else
  					if(param instanceof long[])
  						paramVal.append(Arrays.toString((long[])param));
  					else
      				paramVal.append(param);
  				}
  				else
    				paramVal.append(param);
  			}
  			else
  				paramVal.append(param);
  		}
  		
  		if (entry)
  			paramVal.append(") ");
  		else
  			paramVal.append(" ");
  		
  		msg = paramVal.toString();
  	}
  	
	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("logFunction");
  	
  	logMsg(LogLevel.FUNCTION, callerNames, msg);
  }
  
  public void logDebug(String msg, Object... msgArgs) {
  	
  	if (!checkLogLevel(LogLevel.DEBUG, this))
      return;

	  // Get the package, class, and method names.
	  String[] callerNames = getCallerMethodName("logDebug");
  	
  	logMsg(LogLevel.DEBUG, callerNames, msg, msgArgs);
  }
  
  public void close() {
  	if (handler != null
  			&& handler instanceof LogFileHandler) {
  		try {
				handler.close();
			} catch (Exception e) {
				// Ignore it.
			}
  	}
  }
  
  public void flush() {
  	if (handler != null)
  			handler.flush();
  }
  
  private void logMsg(LogLevel level, String[] callerNames,
  										String msg, Object... msgArgs) {
    // Log the message.
    String formattedMsg = formatLogMsg(
											        level,
											        callerNames[0],
											        callerNames[1],
											        callerNames[2],
											        msg,
											        msgArgs);
    
    try {
	    if (formattedMsg != null
	    		&& null != handler) {
	    	handler.write(formattedMsg);
	    }
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  private String formatLogMsg(
      LogLevel logLevel,
      String packageName,
      String className,
      String methodName,
      String msg,
      Object... msgArgs)
  {
    if (null == handler)
        return null;

    StringBuffer msgBuf = new StringBuffer();
    SimpleDateFormat dateFormat = null;
	
    dateFormat = new SimpleDateFormat("MMM dd HH:mm:ss.SSS");
	
    dateFormat.format(new Date(), msgBuf, new FieldPosition(0));
    msgBuf.append(" ");
    msgBuf.append(logLevel.toString()).append(" ");
    msgBuf.append(" ");
	
    msgBuf.append("[").append(Thread.currentThread().getId()).append(" ").append(Thread.currentThread().getName()).append("] ");

    msgBuf.append(packageName).append(".");
    msgBuf.append(className).append(".");
    msgBuf.append(methodName).append(": ");

    if (msgArgs == null || msgArgs.length == 0)
    	msgBuf.append(msg);
    else
    	msgBuf.append(new MessageFormat(msg).format(msgArgs));

    return msgBuf.toString();
  }
  
  public static String getLogFileUsingPath(String logLevel, String logPath) {
  	if (logPath == null) {
  		// Check loglevel and get current directory
    	LogLevel level = (logLevel != null )
					? LogLevel.getLogLevel(logLevel)
					: LogLevel.OFF;
			if (level != LogLevel.OFF)
				logPath = System.getProperty("user.dir");
  	}
  	
  	if (logPath != null) {
  		return logPath + File.separatorChar + "redshift_jdbc.log";
  	}
  	else {
  		return null;
  	}
  }
  
}
