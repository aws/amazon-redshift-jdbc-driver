/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.AuthMech;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Properties;

public enum SslMode {
  /**
   * Do not use encrypted connections.
   */
  DISABLE("disable", AuthMech.DISABLE),
  /**
   * Start with non-encrypted connection, then try encrypted one.
   */
  ALLOW("allow", AuthMech.ALLOW),
  /**
   * Start with encrypted connection, fallback to non-encrypted (default).
   */
  PREFER("prefer", AuthMech.PREFER),
  /**
   * Ensure connection is encrypted.
   */
  REQUIRE("require", AuthMech.REQUIRE),
  /**
   * Ensure connection is encrypted, and client trusts server certificate.
   */
  VERIFY_CA("verify-ca", AuthMech.VERIFY_CA),
  /**
   * Ensure connection is encrypted, client trusts server certificate, and server hostname matches
   * the one listed in the server certificate.
   */
  VERIFY_FULL("verify-full", AuthMech.VERIFY_FULL),
  ;

  public static final SslMode[] VALUES = values();

  /**
   * The mode chosen when the caller specifies neither {@code sslmode} nor
   * {@code ssl}. The single place to change to move the default.
   */
  public static final SslMode DEFAULT = VERIFY_FULL;

  public final String value;

  /**
   * The equivalent {@link AuthMech}, stated rather than resolved by name: the two
   * enums are maintained separately, so the compiler should require a counterpart.
   */
  public final AuthMech authMech;

  SslMode(String value, AuthMech authMech) {
    this.value = value;
    this.authMech = authMech;
  }

  public boolean requireEncryption() {
    return this.compareTo(REQUIRE) >= 0;
  }

  public boolean verifyCertificate() {
    return this == VERIFY_CA || this == VERIFY_FULL;
  }

  public boolean verifyPeerName() {
    return this == VERIFY_FULL;
  }

  /**
   * Whether the driver chose the mode rather than the caller. Only a defaulted mode
   * may tolerate a hostname mismatch, so an explicit verify-full still fails.
   *
   * @param info connection properties
   * @return true when neither sslmode nor its authmech alias was supplied
   */
  public static boolean wasModeChosenByDriver(Properties info) {
    return RedshiftProperty.SSL_MODE.get(info) == null
        && RedshiftProperty.AUTH_MECH.get(info) == null;
  }

  public static SslMode of(Properties info) throws RedshiftException {
    String sslmodeProp = RedshiftProperty.SSL_MODE.get(info);
    String authMechProp = RedshiftProperty.AUTH_MECH.get(info);
    String sslmode = (sslmodeProp != null) ? sslmodeProp : authMechProp;
    
    // If sslmode is not set, fallback to ssl parameter
    if (sslmode == null) {
      if (RedshiftProperty.SSL.getBoolean(info) || "".equals(RedshiftProperty.SSL.get(info))) {
        return DEFAULT;
      }
      String iamAuthStr = RedshiftProperty.IAM_AUTH.get(info);
      Boolean iamAuth = (iamAuthStr == null) ? false : Boolean.parseBoolean(iamAuthStr);
      
      return (iamAuth) ? PREFER : DISABLE;
    }

    for (SslMode sslMode : VALUES) {
      if (sslMode.value.equalsIgnoreCase(sslmode)) {
        return sslMode;
      }
    }
    throw new RedshiftException(GT.tr("Invalid sslmode value: {0}", sslmode),
        RedshiftState.CONNECTION_UNABLE_TO_CONNECT);
  }
}
