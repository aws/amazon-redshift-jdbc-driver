/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

/**
 * <p>Helper class to handle boolean type of Redshift.</p>
 *
 * <p>Based on values accepted by the Redshift server:
 * https://www.postgresql.org/docs/current/static/datatype-boolean.html</p>
 */
class BooleanTypeUtil {

  private BooleanTypeUtil() {
  }

  /**
   * Cast an Object value to the corresponding boolean value.
   *
   * @param in Object to cast into boolean
   * @return boolean value corresponding to the cast of the object
   * @throws RedshiftException RedshiftState.CANNOT_COERCE
   */
  static boolean castToBoolean(final Object in) throws RedshiftException {
    if (in instanceof Boolean) {
      return (Boolean) in;
    }
    if (in instanceof String) {
      return fromString((String) in);
    }
    if (in instanceof Character) {
      return fromCharacter((Character) in);
    }
    
    if (in instanceof Number) {
      return fromNumber((Number) in);
    }
    
    throw new RedshiftException("Cannot cast to boolean", RedshiftState.CANNOT_COERCE);
  }

  private static boolean fromString(final String strval) throws RedshiftException {
    // Leading or trailing whitespace is ignored, and case does not matter.
    final String val = strval.trim();
    if ("1".equals(val) || "1.0".equals(val)
    		|| "true".equalsIgnoreCase(val)
        || "t".equalsIgnoreCase(val) || "yes".equalsIgnoreCase(val)
        || "y".equalsIgnoreCase(val) || "on".equalsIgnoreCase(val)) {
      return true;
    }
    if ("0".equals(val) || "0.0".equals(val)
    		|| "false".equalsIgnoreCase(val)
        || "f".equalsIgnoreCase(val) || "no".equalsIgnoreCase(val)
        || "n".equalsIgnoreCase(val) || "off".equalsIgnoreCase(val)) {
      return false;
    }
    
    try {
      return (!val.equalsIgnoreCase("false") &&
          !val.equals("0") &&
          !val.equals("0.0") &&
          !val.equalsIgnoreCase("f"));
    }catch(Exception ex) {
    	throw cannotCoerceException(strval);
    }
  }

  private static boolean fromCharacter(final Character charval) throws RedshiftException {
    if ('1' == charval || 't' == charval || 'T' == charval
        || 'y' == charval || 'Y' == charval) {
      return true;
    }
    if ('0' == charval || 'f' == charval || 'F' == charval
        || 'n' == charval || 'N' == charval) {
      return false;
    }
    throw cannotCoerceException(charval);
  }

  private static boolean fromNumber(final Number numval) throws RedshiftException {
    // Handles BigDecimal, Byte, Short, Integer, Long Float, Double
    // based on the widening primitive conversions.
    final double value = numval.doubleValue();
    if (value == 1.0d) {
      return true;
    }
    if (value == 0.0d) {
      return false;
    }
    
    try {
	    String str = String.valueOf(numval);
	    return fromString(str);
    }
    catch(Exception ex) {
    	throw cannotCoerceException(numval);
    }
  }

  private static RedshiftException cannotCoerceException(final Object value) {
    return new RedshiftException(GT.tr("Cannot cast to boolean: \"{0}\"", String.valueOf(value)),
        RedshiftState.CANNOT_COERCE);
  }

}
