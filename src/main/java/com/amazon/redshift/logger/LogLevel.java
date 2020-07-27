package com.amazon.redshift.logger;

import java.util.ArrayList;

public enum LogLevel {
  /*
   * OFF < ERROR < INFO < FUNCTION < DEBUG 
   */
  OFF,
  ERROR,
  INFO,
  FUNCTION,
  DEBUG;

  private static ArrayList<String> names = new ArrayList<String>();

  static
  {
      names.add("OFF");
      names.add("ERROR");
      names.add("INFO");
      names.add("FUNCTION");
      names.add("DEBUG");
  }

  public static LogLevel getLogLevel(int level)
  {
      switch (level)
      {
		      case 0:
		      {
		          return LogLevel.OFF;
		      }
		      
          case 1:
          {
              return LogLevel.ERROR;
          }

          case 2:
          {
              return LogLevel.INFO;
          }

          case 3:
          {
              return LogLevel.FUNCTION;
          }

          case 4:
          case 5: // TRACE for backward compatibility
          case 6: // DEBUG for backward compatibility
          {
              return LogLevel.DEBUG;
          }

          default:
          {
              return LogLevel.OFF;
          }
      }
  }

  public static LogLevel getLogLevel(String level)
  {
      LogLevel logLevel = LogLevel.OFF;

      if ((null == level) || level.equals(""))
      {
          return logLevel;
      }

      if (level.equalsIgnoreCase("OFF"))
      {
      	logLevel = LogLevel.OFF;
      }
      else if (level.equalsIgnoreCase("ERROR"))
      {
      	logLevel = LogLevel.ERROR;
      }
      else if (level.equalsIgnoreCase("INFO"))
      {
      	logLevel = LogLevel.INFO;
      }
      else if (level.equalsIgnoreCase("FUNCTION"))
      {
      	logLevel = LogLevel.FUNCTION;
      }
      else if (level.equalsIgnoreCase("DEBUG")
      					|| level.equalsIgnoreCase("TRACE")) // TRACE is for backward compatibility
      {
      	logLevel = LogLevel.DEBUG;
      }
      else
      {
          try
          {
              logLevel = getLogLevel(Integer.parseInt(level));
          }
          catch (NumberFormatException e)
          {
          	// Ignore
          }
      }

      return logLevel;
  }
}
