/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;
import java.util.Properties;
import java.sql.SQLWarning;


import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftPropertyMaxResultBufferParser;
/**
 * Wrapper class for SQLWarnings that provides an optimisation to add
 * new warnings to the tail of the SQLWarning singly linked list, avoiding Θ(n) insertion time
 * of calling #setNextWarning on the head. By encapsulating this into a single object it allows
 * users(ie PgStatement) to atomically set and clear the warning chain.
 */
public class RedshiftWarningWrapper {

  private SQLWarning firstWarning;
  private SQLWarning lastWarning;
  private int maxWarningCount;
  private int count;
  private SQLWarning finalWarning;

  public RedshiftWarningWrapper(SQLWarning warning, Properties props) {
  
    String warningCountStr = (props != null) ? 
                                    RedshiftProperty.MAX_WARNING_COUNT.get(props):
                                    RedshiftProperty.MAX_WARNING_COUNT.getDefaultValue();

    try {
      maxWarningCount = Integer.parseInt(warningCountStr);
    } catch (NumberFormatException e) {
      maxWarningCount = Integer.parseInt(RedshiftProperty.MAX_WARNING_COUNT.getDefaultValue());
    }

    if (maxWarningCount >= 0) {
      this.firstWarning = null;
      this.lastWarning = null;
      this.count = 0;
      finalWarning = new SQLWarning("[Redshift JDBC Driver] Additional warnings are truncated " +
                                    "when the number of all warnings exceeds " + maxWarningCount + 
                                    ". The maximum number of warnings can be set using \""+ 
                                    RedshiftProperty.MAX_WARNING_COUNT.getName() + "\" through " +
                                  "driver connection option.", "00000", 0);    
    }

    appendWarning(warning);
  }

  private int getWarningChainSize(SQLWarning head) {
    SQLWarning next = head;
    int chainSize = 0;
    while (next != null) {
      chainSize += 1;
      next = next.getNextWarning();
    }
    return chainSize;
  }

  private void addFinalWarning() {
    if (firstWarning == null) {
      firstWarning = finalWarning;
    }
    else { // lastWarning is null if and only if firstWarning is null
      lastWarning.setNextWarning(finalWarning);
    }
    lastWarning = finalWarning;
    // should skip following appendings
    count = maxWarningCount;
    
  }

  public void appendWarning(SQLWarning sqlWarning) {
    // early return: null input or size limit hit
    if (sqlWarning == null || 
        (maxWarningCount >= 0 && count >= maxWarningCount)) {
      return;
    }
    // consider incoming warning already a chain of multiple warnings  
    int newWarningSize = getWarningChainSize(sqlWarning);

    if (count + newWarningSize < maxWarningCount || maxWarningCount < 0) {
      if (firstWarning == null) {
        firstWarning = sqlWarning;
      }
      else {
        lastWarning.setNextWarning(sqlWarning);
      }
      lastWarning = sqlWarning;
      SQLWarning tmpWarning = lastWarning;
      while (tmpWarning != null) {
        lastWarning = tmpWarning;
        tmpWarning = tmpWarning.getNextWarning();
      }
      count += newWarningSize;
    } else {
      addFinalWarning();
    }

  }

  public SQLWarning getFirstWarning() {
    return firstWarning;
  }

  public void clearWarning() {
    firstWarning = null;
    lastWarning = null;
    count = 0;
  }

}
