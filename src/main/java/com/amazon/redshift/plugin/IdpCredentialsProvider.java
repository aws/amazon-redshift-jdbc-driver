package com.amazon.redshift.plugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.ssl.NonValidatingFactory;

abstract class IdpCredentialsProvider {

  protected static final String KEY_SSL_INSECURE = "ssl_insecure";
  protected boolean m_sslInsecure;
  protected static final Pattern IAM_URL_PATTERN = Pattern.compile("^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']");
  protected static final Pattern IAM_HTTP_URL_PATTERN = Pattern.compile("^(http)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']");
  protected RedshiftLogger m_log;
  
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

  protected void validateURL(String paramString) throws IOException {
    
    URI authorizeRequestUrl = URI.create(paramString);
    String error = "Invalid url:" + paramString;
    
    if(RedshiftLogger.isEnable())
      m_log.log(LogLevel.DEBUG,
          String.format("URI: \n%s", authorizeRequestUrl.toString())
          );
    try
    {
      if(!authorizeRequestUrl.toURL().getProtocol().equalsIgnoreCase("https"))
      {
        m_log.log(LogLevel.ERROR, error);
        
        throw new IOException(error);
      }
      
      Matcher matcher = IAM_URL_PATTERN.matcher(paramString);
      if (!matcher.find())
      {
        m_log.log(LogLevel.ERROR, "Pattern matching failed:" + error);
        
        throw new IOException("Pattern matching failed:" + error);
      }
    }
    catch (MalformedURLException e) 
    {
      throw new IOException(error + " " + e.getMessage(), e);
    } 
  }
  
  
}
