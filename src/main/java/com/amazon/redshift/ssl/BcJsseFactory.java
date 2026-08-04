/*
 * Copyright (c) Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.ssl;

import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * SSL socket factory backed by the BouncyCastle JSSE provider (BCJSSE).
 *
 * <p>Used when {@code preferPQ=true} (the default) so that the resulting
 * {@link javax.net.ssl.SSLSocket} actually understands hybrid post-quantum
 * named groups (X25519MLKEM768, SecP256r1MLKEM768) on Java 8 - 26 where the
 * built-in SunJSSE provider does not.
 *
 * <p>Why this is necessary: {@code SSLSocketFactory.getDefault()} (and any
 * {@code SSLContext.getInstance("TLSv1.3")} call without an explicit provider
 * argument) returns a SunJSSE-backed factory on Oracle / OpenJDK builds.
 * SunJSSE does not implement ML-KEM groups before Java 27, so calling
 * {@code SSLParameters.setNamedGroups(...)} with hybrid PQ names on a SunJSSE
 * socket is silently ignored. BCJSSE, by contrast, reads
 * {@code SSLParameters.getNamedGroups()} via reflection (see
 * {@code org.bouncycastle.jsse.provider.SSLParametersUtil}) and applies the
 * groups when constructing the ClientHello, even on Java 8 - 26, provided
 * BouncyCastle's own ML-KEM implementation is on the classpath (which it is
 * via {@code BouncyCastleCrypto}).
 *
 * <p>The trust managers and key manager are supplied by {@link LibPQFactory}
 * so that sslMode, sslrootcert, sslTrustStorePath, and mTLS client-cert
 * configuration continue to apply unchanged.
 *
 * <p>This class is package-private and is constructed only by {@link MakeSSL}
 * when the underlying SSL factory is the default {@link LibPQFactory} or
 * {@link NonValidatingFactory}. Customer-provided {@code SSL_FACTORY}
 * implementations are never wrapped.
 */
final class BcJsseFactory extends WrappedFactory {

  /** JCA provider name registered by {@code BouncyCastleJsseProvider}. */
  private static final String BCJSSE_PROVIDER_NAME = "BCJSSE";

  /**
   * Build a BCJSSE-backed SSL socket factory using the supplied trust
   * managers and (optional) key manager.
   *
   * @param keyManager   client-side {@link KeyManager} for mTLS, or {@code null}
   *                     when no client certificate is configured
   * @param trustManagers trust managers to use (never {@code null})
   * @throws RedshiftException if BCJSSE is not registered, no compatible TLS
   *                           context is available from BCJSSE, or context
   *                           initialization fails
   */
  BcJsseFactory(KeyManager keyManager, TrustManager[] trustManagers) throws RedshiftException {
    SSLContext ctx = createBcJsseContext();
    KeyManager[] kms = (keyManager == null) ? null : new KeyManager[]{keyManager};
    try {
      ctx.init(kms, trustManagers, null);
    } catch (KeyManagementException ex) {
      throw new RedshiftException(
          GT.tr("Could not initialize BCJSSE SSL context for post-quantum TLS."),
          RedshiftState.CONNECTION_FAILURE, ex);
    }
    factory = ctx.getSocketFactory();
  }

  /**
   * Obtain a {@link SSLContext} from the BCJSSE provider. Tries TLSv1.3 first
   * and falls back to TLSv1.2, mirroring {@link SSLUtil#createSecureSSLContext()}
   * but pinned to the BCJSSE provider so the resulting socket factory is
   * BCJSSE-backed (not SunJSSE).
   */
  private static SSLContext createBcJsseContext() throws RedshiftException {
    try {
      try {
        return SSLContext.getInstance("TLSv1.3", BCJSSE_PROVIDER_NAME);
      } catch (NoSuchAlgorithmException tls13Missing) {
        return SSLContext.getInstance("TLSv1.2", BCJSSE_PROVIDER_NAME);
      }
    } catch (NoSuchAlgorithmException ex) {
      throw new RedshiftException(
          GT.tr("BCJSSE provider does not support TLSv1.3 or TLSv1.2."),
          RedshiftState.CONNECTION_FAILURE, ex);
    } catch (NoSuchProviderException ex) {
      throw new RedshiftException(
          GT.tr("BCJSSE provider is not registered; cannot enable post-quantum TLS."),
          RedshiftState.CONNECTION_FAILURE, ex);
    }
  }
}
