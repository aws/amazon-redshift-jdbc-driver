package com.amazon.redshift.util;

public class RedshiftGeometry {
	
  /**
   * Look up for ascii value DEL
   */
  private static final byte asciiInvalidValue = 0x7f;

  /**
   * Helper method to turn input EWKT into binary format
   *
   * @param bytes              The input data.
   * @param beginIndex         The starting index.
   * @param length             The number of characters to consume.
   *
   * @return transformed byte array
   */
  public static byte[] transformEWKTFormat(byte[] bytes, int beginIndex, int length)
  {
      boolean errorFlag = false;
      int pointer = beginIndex;
      byte[] result;

      if (null == bytes )
      {
          return null;
      }
      else if (0 == length)
      {
          return new byte[0];
      }
      else
      {
          // EWKT is always hex encoded
          // Check to see if byte array is of expected length
          if (1 == (beginIndex + length-pointer) % 2)
          {
              byte[] newArray = new byte[length];
              System.arraycopy(bytes, beginIndex, newArray, 0, length);
              return newArray;
          }

          result = new byte[(beginIndex + length-pointer) / 2];
          errorFlag = false;

          for (int i = 0 ; pointer < beginIndex + length ; i++)
          {
              // Get the ascii number encoded
              int stage = hexEncodingLookupNoCase(bytes[pointer]) << 4;
              // Error Check
              errorFlag = ((byte)stage == asciiInvalidValue) || errorFlag;
              pointer++;
              int stage2 =hexEncodingLookupNoCase(bytes[pointer]);
              errorFlag = ((byte)stage2 == asciiInvalidValue) || errorFlag;
              pointer++;

              result[i] = (byte)(stage | stage2);
          }

          if(errorFlag)
          {
              byte[] newArray = new byte[length];
              System.arraycopy(bytes, beginIndex, newArray, 0, length);
              return newArray;
          }

          return result;
      }
  }
  
  /**
   * Look up method to return the ascii value from a ascii encoding
   *
   * @param bytes              The intput data
   *
   * @return value represented by byte
   */
  private static int hexEncodingLookupNoCase(byte inputValue)
  {
      byte result = asciiInvalidValue;
      switch (inputValue)
      {
          case 48:
              result = 0x00;
              break;
          case 49:
              result = 0x01;
              break;
          case 50:
              result = 0x02;
              break;
          case 51:
              result = 0x03;
              break;
          case 52:
              result = 0x04;
              break;
          case 53:
              result = 0x05;
              break;
          case 54:
              result = 0x06;
              break;
          case 55:
              result = 0x07;
              break;
          case 56:
              result = 0x08;
              break;
          case 57:
              result = 0x09;
              break;
          case 65:
          case 97:
              result = 0x0a;
              break;
          case 66:
          case 98:
              result = 0x0b;
              break;
          case 67:
          case 99:
              result = 0x0c;
              break;
          case 68:
          case 100:
              result = 0x0d;
              break;
          case 69:
          case 101:
              result = 0x0e;
              break;
          case 70:
          case 102:
              result = 0x0f;
              break;
      }
      return result;
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
}
