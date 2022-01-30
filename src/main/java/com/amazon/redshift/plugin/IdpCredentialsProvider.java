package com.amazon.redshift.plugin;

import java.security.GeneralSecurityException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;

import com.amazon.redshift.ssl.NonValidatingFactory;

abstract class IdpCredentialsProvider {

  protected static final String KEY_SSL_INSECURE = "ssl_insecure";
  protected boolean m_sslInsecure;
  
  protected CloseableHttpClient getHttpClient() throws GeneralSecurityException
  {
      RequestConfig rc = RequestConfig.custom()
              .setSocketTimeout(60000)
              .setConnectTimeout(60000)
              .setExpectContinueEnabled(false)
              .setCookieSpec(CookieSpecs.STANDARD)
              .build();

      HttpClientBuilder builder = HttpClients.custom()
              .setDefaultRequestConfig(rc)
              .setRedirectStrategy(new LaxRedirectStrategy())
              .useSystemProperties(); // this is needed for proxy setting using system properties.

      if (m_sslInsecure)
      {
          SSLContext ctx = SSLContext.getInstance("TLSv1.2");
          TrustManager[] tma = new TrustManager[]{ new NonValidatingFactory.NonValidatingTM()};
          ctx.init(null, tma, null);
          SSLSocketFactory factory = ctx.getSocketFactory();

          SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(
                  factory,
                  new NoopHostnameVerifier());

          builder.setSSLSocketFactory(sf);
      }

      return builder.build();
  }
  
}
