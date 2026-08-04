/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ssl;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.core.SocketFactoryFactory;
import com.amazon.redshift.jdbc.SslMode;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.ObjectFactory;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

public class MakeSSL extends ObjectFactory {

  /**
   * Hybrid post-quantum key-exchange groups offered in the TLS 1.3
   * ClientHello when {@code preferPQ=true} (the default), listed in
   * preference order (PQ first, then classical).
   */
  private static final String[] PQ_NAMED_GROUPS = {
      "X25519MLKEM768", "SecP256r1MLKEM768", "x25519", "secp256r1", "secp384r1"
  };

  public static void convert(RedshiftStream stream, Properties info)
      throws RedshiftException, IOException {

    SSLSocketFactory factory = SocketFactoryFactory.getSslSocketFactory(info);
    // Hold on to the original factory so we can still propagate any
    // LazyKeyManager exception from LibPQFactory after we (potentially) swap
    // the factory used to create the socket.
    SSLSocketFactory originalFactory = factory;
    boolean preferPQ = Boolean.parseBoolean(RedshiftProperty.PREFER_PQ.get(info));

    // When preferPQ=true (default) AND the customer has not provided their
    // own SSL_FACTORY, route through BCJSSE so that the SSLSocket actually
    // honours hybrid post-quantum named groups on Java 20 - 26. SunJSSE
    // (the default JSSE) silently drops unknown groups, which is why the
    // earlier "set named groups via reflection on a default-JSSE socket"
    // approach was a no-op in practice.
    if (preferPQ && isDefaultFactory(factory)) {
      SSLSocketFactory bcFactory = tryBuildBcJsseFactory(factory);
      if (bcFactory != null) {
        factory = bcFactory;
      } else if (RedshiftLogger.isEnable()) {
        RedshiftLogger logger = RedshiftLogger.getDriverLogger();
        if (logger != null) {
          logger.log(LogLevel.DEBUG,
              "BCJSSE unavailable; PQ key exchange disabled, using classical ECDHE.");
        }
      }
    }

    SSLSocket newConnection;
    try {
      newConnection = (SSLSocket) factory.createSocket(stream.getSocket(),
          stream.getHostSpec().getHost(), stream.getHostSpec().getPort(), true);
      // We must invoke manually, otherwise the exceptions are hidden
      newConnection.setUseClientMode(true);
      maybeApplyPqPolicy(newConnection, info);
      newConnection.startHandshake();
    } catch (IOException ex) {
      throw new RedshiftException(GT.tr("SSL error: {0}", ex.getMessage()),
          RedshiftState.CONNECTION_FAILURE, ex);
    }
    if (originalFactory instanceof LibPQFactory) { // throw any KeyManager exception
      ((LibPQFactory) originalFactory).throwKeyManagerException();
    }

    SslMode sslMode = SslMode.of(info);
    if (sslMode.verifyPeerName()) {
      verifyPeerName(stream, info, newConnection);
    }

    stream.changeSocket(newConnection, false, info);
  }

  /**
   * @return {@code true} when the supplied factory is one the driver itself
   *     produced from the default code path (LibPQFactory or
   *     NonValidatingFactory). Customer-supplied {@code SSL_FACTORY}
   *     classes return {@code false} and are never wrapped, since the
   *     customer has made an explicit choice we should not silently
   *     override.
   */
  private static boolean isDefaultFactory(SSLSocketFactory factory) {
    return factory instanceof LibPQFactory || factory instanceof NonValidatingFactory;
  }

  /**
   * Build a BCJSSE-backed factory that reuses the trust managers and key
   * manager already configured on {@code source} (so sslMode, sslrootcert,
   * sslTrustStorePath, and mTLS client certs keep working). Returns
   * {@code null} on best-effort failure (BCJSSE not on the classpath, BC
   * provider could not be registered, no compatible TLS protocol from
   * BCJSSE, or unexpected error). Callers fall back to the original
   * factory.
   */
  private static SSLSocketFactory tryBuildBcJsseFactory(SSLSocketFactory source) {
    if (!BcProviderRegistrar.ensureInstalled()) {
      return null;
    }
    try {
      if (source instanceof LibPQFactory) {
        LibPQFactory libpq = (LibPQFactory) source;
        return new BcJsseFactory(libpq.getKeyManager(), libpq.getTrustManagers());
      }
      if (source instanceof NonValidatingFactory) {
        // Mirror NonValidatingFactory's trust posture (server validation
        // disabled) but build the socket from BCJSSE so PQ groups apply.
        return new BcJsseFactory(null,
            new TrustManager[]{new NonValidatingFactory.NonValidatingTM()});
      }
      return null;
    } catch (Throwable t) {
      if (RedshiftLogger.isEnable()) {
        RedshiftLogger logger = RedshiftLogger.getDriverLogger();
        if (logger != null) {
          logger.log(LogLevel.DEBUG,
              "BCJSSE factory creation failed; falling back to classical ECDHE: {0}",
              t.toString());
        }
      }
      return null;
    }
  }

  /**
   * When {@code preferPQ=true} (default), call
   * {@code SSLParameters.setNamedGroups} with PQ hybrid groups ordered ahead
   * of classical groups so the ClientHello advertises them. Best-effort: if
   * the runtime lacks {@code setNamedGroups} (Java &lt; 20) the call is
   * silently skipped and the handshake proceeds with classical ECDHE.
   *
   * <p>This method only sets the named groups on the {@link SSLParameters}
   * object. Whether those groups actually end up in the ClientHello depends
   * on the JSSE provider that produced {@code socket}:
   * <ul>
   *   <li>BCJSSE (used when {@code preferPQ=true} and the default factory is
   *       in effect — see {@link #convert}) honours
   *       {@code SSLParameters.getNamedGroups()} via reflection on Java
   *       20+ and applies the groups when constructing the ClientHello.</li>
   *   <li>SunJSSE on Java &lt; 27 silently ignores ML-KEM hybrid group names
   *       because it has no implementation for them.</li>
   *   <li>SunJSSE on Java 27+ is expected to recognise the hybrid groups
   *       natively (roadmap, not yet shipped).</li>
   * </ul>
   * Customer-supplied {@code SSL_FACTORY} classes are not swapped to BCJSSE
   * (we treat that as an explicit opt-out from the driver's default TLS
   * configuration), so on those code paths this method is a best-effort
   * no-op until SunJSSE adds native PQ support.
   *
   * <p>Reflection is used to stay source-compatible with Java 8 (the driver's
   * build target), since {@code SSLParameters.setNamedGroups} landed in
   * Java 20.
   *
   * <p>Package-private for unit testing.
   */
  static void maybeApplyPqPolicy(SSLSocket socket, Properties info) {
    if (!Boolean.parseBoolean(RedshiftProperty.PREFER_PQ.get(info))) {
      return;
    }
    BcProviderRegistrar.ensureInstalled();
    try {
      SSLParameters params = socket.getSSLParameters();
      Method m = SSLParameters.class.getMethod("setNamedGroups", String[].class);
      m.invoke(params, (Object) PQ_NAMED_GROUPS);
      socket.setSSLParameters(params);
    } catch (Throwable t) {
      if (RedshiftLogger.isEnable()) {
        RedshiftLogger logger = RedshiftLogger.getDriverLogger();
        if (logger != null) {
          logger.log(LogLevel.DEBUG,
              "setNamedGroups reflective call failed; falling back to classical ECDHE: {0}",
              t.toString());
        }
      }
    }
  }

  private static void verifyPeerName(RedshiftStream stream, Properties info, SSLSocket newConnection)
      throws RedshiftException {
    HostnameVerifier hvn;
    String sslhostnameverifier = RedshiftProperty.SSL_HOSTNAME_VERIFIER.get(info);
    if (sslhostnameverifier == null) {
      hvn = RedshiftjdbcHostnameVerifier.INSTANCE;
      sslhostnameverifier = "RedshiftjdbcHostnameVerifier";
    } else {
      try {
        hvn = instantiate(HostnameVerifier.class, sslhostnameverifier, info, false, null);
      } catch (Exception e) {
        throw new RedshiftException(
            GT.tr("The HostnameVerifier class provided {0} could not be instantiated.",
                sslhostnameverifier),
            RedshiftState.CONNECTION_FAILURE, e);
      }
    }

    if (hvn.verify(stream.getHostSpec().getHost(), newConnection.getSession())) {
      return;
    }

    throw new RedshiftException(
        GT.tr("The hostname {0} could not be verified by hostnameverifier {1}.",
            stream.getHostSpec().getHost(), sslhostnameverifier),
        RedshiftState.CONNECTION_FAILURE);
  }

}
