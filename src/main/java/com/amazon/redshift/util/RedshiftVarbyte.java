package com.amazon.redshift.util;

import java.sql.SQLException;

public class RedshiftVarbyte {
	
  /*
   * Converts a RS varbyte raw value (i.e. the raw binary representation of the varbyte data type) into
   * a java byte[]
   */
  public static byte[] toBytes(byte[] s) throws SQLException {
    if (s == null) {
      return null;
    }
    
    return toBytesFromHex(s);
  }
  
  public static String convertToString(byte[] data) {
    char[] hex = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[2 * data.length];
    for (int i = 0; i < data.length; i++)
    {
        int v = data[i] & 0xFF;
        hexChars[i * 2] = hex[v >>> 4];
        hexChars[i * 2 + 1] = hex[v & 0x0F];
    }
    
    return new String(hexChars);
  }
  
  private static byte[] toBytesFromHex(byte[] s) {
    byte[] output = new byte[(s.length) / 2];
    for (int i = 0; i < output.length; i++) {
      byte b1 = gethex(s[i * 2]);
      byte b2 = gethex(s[i * 2 + 1]);
      // squid:S3034
      // Raw byte values should not be used in bitwise operations in combination with shifts
      output[i] = (byte) ((b1 << 4) | (b2 & 0xff));
    }
    return output;
  }
  
  private static byte gethex(byte b) {
    // 0-9 == 48-57
    if (b <= 57) {
      return (byte) (b - 48);
    }

    // a-f == 97-102
    if (b >= 97) {
      return (byte) (b - 97 + 10);
    }

    // A-F == 65-70
    return (byte) (b - 65 + 10);
  }
}
