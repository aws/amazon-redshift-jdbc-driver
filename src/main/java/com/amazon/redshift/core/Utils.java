/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package com.amazon.redshift.core;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

/**
 * Collection of utilities used by the protocol-level code.
 */
public class Utils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Turn a bytearray into a printable form, representing each byte in hex.
   *
   * @param data the bytearray to stringize
   * @return a hex-encoded printable representation of {@code data}
   */
  public static String toHexString(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length * 2);
    for (byte element : data) {
      sb.append(Integer.toHexString((element >> 4) & 15));
      sb.append(Integer.toHexString(element & 15));
    }
    return sb.toString();
  }

  /**
   * Keep a local copy of the UTF-8 Charset so we can avoid synchronization overhead from looking up
   * the Charset by name as String.getBytes(String) requires.
   */
  private static final Charset utf8Charset = Charset.forName("UTF-8");

  /**
   * Encode a string as UTF-8.
   *
   * @param str the string to encode
   * @return the UTF-8 representation of {@code str}
   */
  public static byte[] encodeUTF8(String str) {
    // See com.amazon.redshift.benchmark.encoding.UTF8Encoding#string_getBytes
    // for performance measurements.
    // In OracleJDK 6u65, 7u55, and 8u40 String.getBytes(Charset) is
    // 3 times faster than other JDK approaches.
    return str.getBytes(utf8Charset);
  }

  /**
   * Escape the given literal {@code value} and append it to the string builder {@code sbuf}. If
   * {@code sbuf} is {@code null}, a new StringBuilder will be returned. The argument
   * {@code standardConformingStrings} defines whether the backend expects standard-conforming
   * string literals or allows backslash escape sequences.
   *
   * @param sbuf the string builder to append to; or {@code null}
   * @param value the string value
   * @param standardConformingStrings if standard conforming strings should be used
   * @param onlyQuotes only escape quote and not the backslash for database name
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a {@code \0} character
   */
  public static StringBuilder escapeLiteral(StringBuilder sbuf, String value,
      boolean standardConformingStrings, boolean onlyQuotes) throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder((value.length() + 10) / 10 * 11); // Add 10% for escaping.
    }
    doAppendEscapedLiteral(sbuf, value, standardConformingStrings, onlyQuotes);
    return sbuf;
  }

  public static StringBuilder escapeLiteral(StringBuilder sbuf, String value,
      boolean standardConformingStrings) throws SQLException {
  	return escapeLiteral(sbuf, value, standardConformingStrings, false);
  }
  
  /**
   * Common part for {@link #escapeLiteral(StringBuilder, String, boolean)}.
   *
   * @param sbuf Either StringBuffer or StringBuilder as we do not expect any IOException to be
   *        thrown
   * @param value value to append
   * @param standardConformingStrings if standard conforming strings should be used
   */
  private static void doAppendEscapedLiteral(Appendable sbuf, String value,
      boolean standardConformingStrings, boolean onlyQuote) throws SQLException {
    try {
      if (standardConformingStrings) {
        // With standard_conforming_strings on, escape only single-quotes.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new RedshiftException(GT.tr("Zero bytes may not occur in string parameters."),
                RedshiftState.INVALID_PARAMETER_VALUE);
          }
          if (ch == '\'') {
            sbuf.append('\'');
          }
          sbuf.append(ch);
        }
      } else {
        // With standard_conforming_string off, escape backslashes and
        // single-quotes, but still escape single-quotes by doubling, to
        // avoid a security hazard if the reported value of
        // standard_conforming_strings is incorrect, or an error if
        // backslash_quote is off.
        for (int i = 0; i < value.length(); ++i) {
          char ch = value.charAt(i);
          if (ch == '\0') {
            throw new RedshiftException(GT.tr("Zero bytes may not occur in string parameters."),
                RedshiftState.INVALID_PARAMETER_VALUE);
          }
          
          if(onlyQuote) {
            if (ch == '\'') {
              sbuf.append(ch);
            }
          }
          else
          if (ch == '\\' || ch == '\'') {
            sbuf.append(ch);
          }
          
          sbuf.append(ch);
        }
      }
    } catch (IOException e) {
      throw new RedshiftException(GT.tr("No IOException expected from StringBuffer or StringBuilder"),
          RedshiftState.UNEXPECTED_ERROR, e);
    }
  }

  /**
   * Escape the given identifier {@code value} and append it to the string builder {@code sbuf}.
   * If {@code sbuf} is {@code null}, a new StringBuilder will be returned. This method is
   * different from appendEscapedLiteral in that it includes the quoting required for the identifier
   * while {@link #escapeLiteral(StringBuilder, String, boolean)} does not.
   *
   * @param sbuf the string builder to append to; or {@code null}
   * @param value the string value
   * @return the sbuf argument; or a new string builder for sbuf == null
   * @throws SQLException if the string contains a {@code \0} character
   */
  public static StringBuilder escapeIdentifier(StringBuilder sbuf, String value)
      throws SQLException {
    if (sbuf == null) {
      sbuf = new StringBuilder(2 + (value.length() + 10) / 10 * 11); // Add 10% for escaping.
    }
    doAppendEscapedIdentifier(sbuf, value);
    return sbuf;
  }

  /**
   * Common part for appendEscapedIdentifier.
   *
   * @param sbuf Either StringBuffer or StringBuilder as we do not expect any IOException to be
   *        thrown.
   * @param value value to append
   */
  private static void doAppendEscapedIdentifier(Appendable sbuf, String value) throws SQLException {
    try {
      sbuf.append('"');

      for (int i = 0; i < value.length(); ++i) {
        char ch = value.charAt(i);
        if (ch == '\0') {
          throw new RedshiftException(GT.tr("Zero bytes may not occur in identifiers."),
              RedshiftState.INVALID_PARAMETER_VALUE);
        }
        if (ch == '"') {
          sbuf.append(ch);
        }
        sbuf.append(ch);
      }

      sbuf.append('"');
    } catch (IOException e) {
      throw new RedshiftException(GT.tr("No IOException expected from StringBuffer or StringBuilder"),
          RedshiftState.UNEXPECTED_ERROR, e);
    }
  }

  /**
   * <p>Attempt to parse the server version string into an XXYYZZ form version number.</p>
   *
   * <p>Returns 0 if the version could not be parsed.</p>
   *
   * <p>Returns minor version 0 if the minor version could not be determined, e.g. devel or beta
   * releases.</p>
   *
   * <p>If a single major part like 90400 is passed, it's assumed to be a pre-parsed version and
   * returned verbatim. (Anything equal to or greater than 10000 is presumed to be this form).</p>
   *
   * <p>The yy or zz version parts may be larger than 99. A NumberFormatException is thrown if a
   * version part is out of range.</p>
   *
   * @param serverVersion server vertion in a XXYYZZ form
   * @return server version in number form
   * @deprecated use specific {@link Version} instance
   */
  @Deprecated
  public static int parseServerVersionStr(String serverVersion) throws NumberFormatException {
    return ServerVersion.parseServerVersionStr(serverVersion);
  }

  public static boolean isNullOrEmpty(String value) {
    return value == null || value.isEmpty();
  }

  /**
   * Parses a JSON string into a JsonNode object.
   *
   * @param json The JSON string to be parsed. Must be a valid JSON format.
   *             Can represent an object, array, or primitive JSON value.
   * @return A JsonNode representing the parsed JSON structure.
   *         The specific type of JsonNode (ObjectNode, ArrayNode, etc.)
   *         will depend on the input JSON structure.
   * @throws JsonProcessingException if the input string is not valid JSON,
   *         or if there are any other problems parsing the JSON content.
   * @throws IllegalArgumentException if the input string is null.
   *
   * @see com.fasterxml.jackson.databind.JsonNode
   * @see com.fasterxml.jackson.databind.ObjectMapper#readTree(String)
   *
   * Example usage:
   * <pre>
   * String jsonString = "{\"name\":\"John\",\"age\":30}";
   * JsonNode node = JsonUtil.parseJson(jsonString);
   * String name = node.get("name").asText();
   * </pre>
   */
  public static JsonNode parseJson(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readTree(json);
  }
}
