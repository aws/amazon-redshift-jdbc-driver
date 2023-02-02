/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Properties;

public enum SslMode {
  /**
   * Do not use encrypted connections.
   */
  DISABLE("disable"),
  /**
   * Start with non-encrypted connection, then try encrypted one.
   */
  ALLOW("allow"),
  /**
   * Start with encrypted connection, fallback to non-encrypted (default).
   */
  PREFER("prefer"),
  /**
   * Ensure connection is encrypted.
   */
  REQUIRE("require"),
  /**
   * Ensure connection is encrypted, and client trusts server certificate.
   */
  VERIFY_CA("verify-ca"),
  /**
   * Ensure connection is encrypted, client trusts server certificate, and server hostname matches
   * the one listed in the server certificate.
   */
  VERIFY_FULL("verify-full"),
  ;

  public static final SslMode[] VALUES = values();

  public final String value;

  SslMode(String value) {
    this.value = value;
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

  public static SslMode of(Properties info) throws RedshiftException {
    String sslmodeProp = RedshiftProperty.SSL_MODE.get(info);
    String authMechProp = RedshiftProperty.AUTH_MECH.get(info);
    String sslmode = (sslmodeProp != null) ? sslmodeProp : authMechProp;
    
    // If sslmode is not set, fallback to ssl parameter
    if (sslmode == null) {
      if (RedshiftProperty.SSL.getBoolean(info) || "".equals(RedshiftProperty.SSL.get(info))) {
        return VERIFY_CA; // VERIFY_FULL;
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
