/*
 * Copyright (c) Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.ssl;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;

import java.security.Provider;
import java.security.Security;

/**
 * Installs BouncyCastle providers (BCPROV + BCJSSE) at runtime, once, on the first
 * connection that has not explicitly opted out via {@code preferPQ=false}. PQ
 * advertising is on by default (requirements.md R3a). Providers are appended with
 * {@link Security#addProvider}, not {@code insertProviderAt(..., 1)}, so the default
 * JCE/JSSE providers keep handling AES-GCM and other hot-path operations. See
 * requirements.md R3/R8.
 */
final class BcProviderRegistrar {

  private static final String BCPROV = "org.bouncycastle.jce.provider.BouncyCastleProvider";
  private static final String BCJSSE = "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider";

  private static volatile boolean installed;
  private static volatile boolean failedPermanently;

  private BcProviderRegistrar() { }

  /** @return true if BCJSSE is available after this call (either just installed or already present). */
  static boolean ensureInstalled() {
    if (installed) return true;
    if (failedPermanently) return false;
    return doInstall();
  }

  private static synchronized boolean doInstall() {
    if (installed) return true;
    if (failedPermanently) return false;
    try {
      // Construct BouncyCastleProvider first.
      Class<?> bcProvClass = Class.forName(BCPROV);
      Provider bcProv = (Provider) bcProvClass.getDeclaredConstructor().newInstance();
      if (Security.getProvider(bcProv.getName()) == null) {
        Security.addProvider(bcProv);
      }

      // Construct BouncyCastleJsseProvider with bcProv explicitly.
      // Using the (Provider) constructor forces BCJSSE to use BC's own
      // ML-KEM implementation rather than delegating to SunJCE (which lacks
      // ML-KEM on JDK <24 and marks the hybrid groups as disabled).
      // See bcgit/bc-java#2252.
      Class<?> bcJsseClass = Class.forName(BCJSSE);
      Provider bcJsse = (Provider) bcJsseClass
          .getDeclaredConstructor(Provider.class)
          .newInstance(bcProv);
      if (Security.getProvider(bcJsse.getName()) == null) {
        Security.addProvider(bcJsse);
      }

      installed = true;
      return true;
    } catch (Throwable t) {
      failedPermanently = true;
      if (RedshiftLogger.isEnable()) {
        RedshiftLogger logger = RedshiftLogger.getDriverLogger();
        if (logger != null) {
          logger.log(LogLevel.DEBUG,
              "BouncyCastle providers unavailable: {0}. PQ key exchange disabled for this JVM.",
              t.toString());
        }
      }
      return false;
    }
  }

}
