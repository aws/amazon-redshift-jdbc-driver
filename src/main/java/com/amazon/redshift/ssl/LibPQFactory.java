/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ssl;

import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.SslMode;
import com.amazon.redshift.ssl.NonValidatingFactory.NonValidatingTM;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.ObjectFactory;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * Provide an SSLSocketFactory that is compatible with the libpq behaviour.
 */
public class LibPQFactory extends WrappedFactory {

  /**
   * The system property to set/get the trustore path.
   */
  private static final String TRUSTSTORE_PROPERTY = "javax.net.ssl.trustStore";

  /**
   * The system property to set/get the trustore passphrase.
   */
  private static final String TRUSTSTORE_PWD_PROPERTY ="javax.net.ssl.trustStorePassword";
	
  KeyManager km;
  boolean defaultfile;

  private CallbackHandler getCallbackHandler(Properties info) throws RedshiftException {
    // Determine the callback handler
    CallbackHandler cbh;
    String sslpasswordcallback = RedshiftProperty.SSL_PASSWORD_CALLBACK.get(info);
    if (sslpasswordcallback != null) {
      try {
        cbh = (CallbackHandler) ObjectFactory.instantiate(sslpasswordcallback, info, false, null);
      } catch (Exception e) {
        throw new RedshiftException(
          GT.tr("The password callback class provided {0} could not be instantiated.",
            sslpasswordcallback),
          RedshiftState.CONNECTION_FAILURE, e);
      }
    } else {
      cbh = new ConsoleCallbackHandler(RedshiftProperty.SSL_PASSWORD.get(info));
    }
    return cbh;
  }

  private void initPk8(String sslkeyfile, String defaultdir, Properties info) throws  RedshiftException {

    // Load the client's certificate and key
    String sslcertfile = RedshiftProperty.SSL_CERT.get(info);
    if (sslcertfile == null) { // Fall back to default
      defaultfile = true;
      sslcertfile = defaultdir + "redshift.crt";
    }

    // If the properties are empty, give null to prevent client key selection
    km = new LazyKeyManager(("".equals(sslcertfile) ? null : sslcertfile),
      ("".equals(sslkeyfile) ? null : sslkeyfile), getCallbackHandler(info), defaultfile);
  }

  private void initP12(String sslkeyfile, Properties info) throws RedshiftException {
    km = new PKCS12KeyManager(sslkeyfile, getCallbackHandler(info));
  }

  /**
   * @param info the connection parameters The following parameters are used:
   *        sslmode,sslcert,sslkey,sslrootcert,sslhostnameverifier,sslpasswordcallback,sslpassword
   * @throws RedshiftException if security error appears when initializing factory
   */
  public LibPQFactory(Properties info) throws RedshiftException {
    try {
      SSLContext ctx = SSLContext.getInstance("TLS"); // or "SSL" ?

      // Determining the default file location
      String pathsep = System.getProperty("file.separator");
      String defaultdir;

      if (System.getProperty("os.name").toLowerCase().contains("windows")) { // It is Windows
        defaultdir = System.getenv("APPDATA") + pathsep + "redshift" + pathsep;
      } else {
        defaultdir = System.getProperty("user.home") + pathsep + ".redshift" + pathsep;
      }

      String sslkeyfile = RedshiftProperty.SSL_KEY.get(info);
      if (sslkeyfile == null) { // Fall back to default
        defaultfile = true;
        sslkeyfile = defaultdir + "redshift.pk8";
      }
      if (sslkeyfile.endsWith("pk8")) {
        initPk8(sslkeyfile, defaultdir, info);
      }

      if (sslkeyfile.endsWith("p12")) {
        initP12(sslkeyfile, info);
      }

      TrustManager[] tm;
      SslMode sslMode = SslMode.of(info);
      if (!sslMode.verifyCertificate()) {
        // server validation is not required
        tm = new TrustManager[]{new NonValidatingTM()};
      } else {
      		String sslTrustStorePath = RedshiftProperty.SSL_TRUSTSTORE_PATH_KEY.get(info);
          String sslrootcertfile = RedshiftProperty.SSL_ROOT_CERT.get(info);
      		String sslTrustStorePwd = RedshiftProperty.SSL_TRUSTSTORE_PWD_KEY.get(info);
          
	        // Load the server certificate
	        if (null != sslTrustStorePath)
	        {
	            tm = getTrustManagerWithDefinedTrustStore(sslTrustStorePath, sslTrustStorePwd);
	        }
	        else if(null != sslrootcertfile)
	        {
	            tm = getTrustManagerWithImportedCertificate(sslrootcertfile);
	        }
	        else
	        {
	            tm = getDefaultTrustManager();
	        } 

/* The original root.crt code start ---
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        KeyStore ks;
        try {
          ks = KeyStore.getInstance("jks");
        } catch (KeyStoreException e) {
          // this should never happen
          throw new NoSuchAlgorithmException("jks KeyStore not available");
        }
//        String sslrootcertfile = RedshiftProperty.SSL_ROOT_CERT.get(info);
        if (sslrootcertfile == null) { // Fall back to default
          sslrootcertfile = defaultdir + "root.crt"; 
        }
        FileInputStream fis;
        try {
          fis = new FileInputStream(sslrootcertfile); // NOSONAR
        } catch (FileNotFoundException ex) {
          throw new RedshiftException(
              GT.tr("Could not open SSL root certificate file {0}.", sslrootcertfile),
              RedshiftState.CONNECTION_FAILURE, ex);
        }
        try {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          // Certificate[] certs = cf.generateCertificates(fis).toArray(new Certificate[]{}); //Does
          // not work in java 1.4
          Object[] certs = cf.generateCertificates(fis).toArray(new Certificate[]{});
          ks.load(null, null);
          for (int i = 0; i < certs.length; i++) {
            ks.setCertificateEntry("cert" + i, (Certificate) certs[i]);
          }
          tmf.init(ks);
        } catch (IOException ioex) {
          throw new RedshiftException(
              GT.tr("Could not read SSL root certificate file {0}.", sslrootcertfile),
              RedshiftState.CONNECTION_FAILURE, ioex);
        } catch (GeneralSecurityException gsex) {
          throw new RedshiftException(
              GT.tr("Loading the SSL root certificate {0} into a TrustManager failed.",
                      sslrootcertfile),
              RedshiftState.CONNECTION_FAILURE, gsex);
        } finally {
          try {
            fis.close();
          } catch (IOException e) {
            // ignore 
          }
        }
        tm = tmf.getTrustManagers();
--- The original root.crt code end. */      
        
      } 

      // finally we can initialize the context
      try {
        ctx.init(new KeyManager[]{km}, tm, null);
      } catch (KeyManagementException ex) {
        throw new RedshiftException(GT.tr("Could not initialize SSL context."),
            RedshiftState.CONNECTION_FAILURE, ex);
      }

      factory = ctx.getSocketFactory();
    } catch (NoSuchAlgorithmException ex) {
      throw new RedshiftException(GT.tr("Could not find a java cryptographic algorithm: {0}.",
              ex.getMessage()), RedshiftState.CONNECTION_FAILURE, ex);
    }
  }

  /**
   * Propagates any exception from {@link LazyKeyManager}.
   *
   * @throws RedshiftException if there is an exception to propagate
   */
  public void throwKeyManagerException() throws RedshiftException {
    if (km != null) {
      if (km instanceof LazyKeyManager) {
        ((LazyKeyManager)km).throwKeyManagerException();
      }
      if (km instanceof PKCS12KeyManager) {
        ((PKCS12KeyManager)km).throwKeyManagerException();
      }
    }
  }
  
  /**
   * Initialize and return the trust manager with SSLtruststore passed in from the user
   *
   * @return TrustManager[]           Array of initialized trust managers
   * @throws RedshiftException
   */
  private TrustManager[] getTrustManagerWithDefinedTrustStore(String sslTrustStorePath, 
  																String sslTrustStorePwd) throws RedshiftException
  {
      // The Keystore containing certificates
      KeyStore truststore = null;

      // The input stream to read in the jks file
      FileInputStream trustStoreSource = null;

      try
      {
          trustStoreSource = new FileInputStream(sslTrustStorePath);

          //Load the trust store using the trust store location provided
          truststore = KeyStore.getInstance(KeyStore.getDefaultType());
          truststore.load(
              trustStoreSource,
              sslTrustStorePwd != null ?
                  sslTrustStorePwd.toCharArray() : null);

          // Initialize the TrustManagerFactory
          TrustManagerFactory tmf =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(truststore);

          return tmf.getTrustManagers();
      }
      catch (Exception e)
      {
        // Error retrieving the available trust managers
      	throw new RedshiftException(
            GT.tr("Error retrieving the available trust managers {0}.", sslTrustStorePath),
            			RedshiftState.CONNECTION_FAILURE, e);
      	
      }
      finally
      {
          if (trustStoreSource != null)
          {
              try
              {
                  trustStoreSource.close();
              }
              catch (IOException e)
              {
              	// Ignore 
              }
          }
      }
  }
  
  /**
   * Returns the trust managers for the temporary Truststore with the given certificate
   * imported into the Truststore.
   *
   * @param keystorePath              The path to the keystore.
   * @param certificatePath           The path to the certificate file. The file must be in either
   *                                  PEM or DER format.
   *
   * @throws RedshiftException           If an error occurs.
   */
  private TrustManager[] getTrustManagerWithImportedCertificate(String sslRootCert)
          throws RedshiftException
  {
      KeyStore truststore = getDefaultKeystore();

      try
      {
          Certificate[] chain = getCertificateChain(sslRootCert);

          // Add the certificate to the truststore.
          truststore.setCertificateEntry(sslRootCert, chain[0]);
      }
      catch (Exception e)
      {
        // Error loading the certificate file.
      	throw new RedshiftException(
            GT.tr("Error loading the certificate file {0}.", sslRootCert),
            			RedshiftState.CONNECTION_FAILURE, e);
      	
      }

      return getTrustManager(truststore);
  }
  
  /**
   * Returns the KeyStore for the given external path.
   *
   * @throws RedshiftException            If an error occurs.
   *
   */
  private KeyStore getDefaultKeystore() throws RedshiftException
  {
      InputStream keystoreStream = null;
      String passphrase = null;


      String keystorePath = System.getProperty(TRUSTSTORE_PROPERTY);
      passphrase = System.getProperty(TRUSTSTORE_PWD_PROPERTY);

      if (null == keystorePath)
      {
          // Default keystore : ${JAVA_HOME}/lib/security/cacerts
          StringBuilder trustorePath = new StringBuilder();
          trustorePath.append(System.getProperty("java.home"));
          trustorePath.append(File.separatorChar);
          trustorePath.append("lib");
          trustorePath.append(File.separatorChar);
          trustorePath.append("security");
          trustorePath.append(File.separatorChar);
          trustorePath.append("cacerts");

          keystorePath = trustorePath.toString();
      }

      try
      {
          keystoreStream = new FileInputStream(new File(keystorePath));
      }
      catch (Exception e)
      {
        // Error retrieving the available trust managers
      	throw new RedshiftException(
            GT.tr("Error loading the keystore  {0}.", keystorePath),
            			RedshiftState.CONNECTION_FAILURE, e);
      }

      try
      {
          // Load the keystore
          KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
          char[] passphraseArray = null;
          if (null != passphrase)
          {
              passphraseArray = passphrase.toCharArray();
          }
          keystore.load(keystoreStream, passphraseArray);
          keystoreStream.close();

          loadDefaultCA(keystore, "redshift.crt");
          loadDefaultCA(keystore, "bjs.redshift.crt");
          loadDefaultCA(keystore, "pdt.redshift.crt");
          return keystore;
      }
      catch (Exception e)
      {
        // Error retrieving the available trust managers
      	throw new RedshiftException(
            GT.tr("Error loading the provided keystore."),
            			RedshiftState.CONNECTION_FAILURE, e);
      }
  }
  
  /**
   * Loads the certificate into the keystore.
   *
   * @param keystore                   The keystore.
   * @param name                       The name of the certificate.
   * @throws IOException               When the file is not found.
   * @throws GeneralSecurityException
   */
  private void loadDefaultCA(KeyStore keystore, String name)
          throws IOException, GeneralSecurityException
  {
      InputStream is = null;

      try
      {
          is = NonValidatingFactory.class.getResourceAsStream(name);

          if (is == null)
          {
              return;
          }

          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          Certificate cert = cf.generateCertificate(is);
          keystore.setCertificateEntry(name, cert);
      }
      finally
      {
          if (is != null)
          {
              is.close();
          }
      }
  }
  
  /**
   * Returns a certificate chain with the certificate found at certificatePath added to it.
   *
   * @param certificatePath   The path to the certificate.
   *
   * @throws RedshiftException   If an error occurs.
   */
  private Certificate[] getCertificateChain(String certificatePath) 
  					throws RedshiftException
  {
      Certificate[] chain = {};
      try
      {
          File certificateFile = new File(certificatePath);
          if (!certificateFile.isFile() || !certificateFile.exists())
          {
            	throw new RedshiftException(
                  GT.tr("Error certificate file doesn't found {0}.", certificatePath),
                  			RedshiftState.CONNECTION_FAILURE);
          }

          InputStream certificateStream = new FileInputStream(certificateFile);
          CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
          chain = certificateFactory.generateCertificates(certificateStream).toArray(chain);
          certificateStream.close();

          if (0 >= chain.length || null == chain[0])
          {
          	throw new RedshiftException(
                GT.tr("Error missing certificate."),
                			RedshiftState.CONNECTION_FAILURE);
          }
      }
      catch (Exception e)
      {
      	throw new RedshiftException(
            GT.tr("Error loading certificate chain."),
            			RedshiftState.CONNECTION_FAILURE, e);
      }

      return chain;
  }
  
  /**
   * Returns the trust managers for the given external Truststore.
   *
   * @param keystore            The keystore
   *
   * @throws RedshiftException            If an error occurs.
   *
   */
  private TrustManager[] getTrustManager(KeyStore keystore)
      throws RedshiftException
  {
      try
      {
          // Initialize the TrustManagerFactory
          TrustManagerFactory tmf =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(keystore);

          return tmf.getTrustManagers();
      }
      catch (Exception e)
      {
        // Error retrieving the available trust managers
      	throw new RedshiftException(
            GT.tr("Error retrieving the available trust managers."),
            			RedshiftState.CONNECTION_FAILURE, e);
      }
  }
  
  /**
   * Returns the trust managers for the trustore.
   *
   * @param keystoreStream            The keystore stream.
   * @param passphrase                The keystore passphrase.
   *
   * @throws RedshiftException            If an error occurs.
   *
   */
  private TrustManager[] getDefaultTrustManager()
      throws RedshiftException
  {
      KeyStore keystore = getDefaultKeystore();
      return getTrustManager(keystore);
  }
  

  /**
   * A CallbackHandler that reads the password from the console or returns the password given to its
   * constructor.
   */
  public static class ConsoleCallbackHandler implements CallbackHandler {

    private char[] password = null;

    ConsoleCallbackHandler(String password) {
      if (password != null) {
        this.password = password.toCharArray();
      }
    }

    /**
     * Handles the callbacks.
     *
     * @param callbacks The callbacks to handle
     * @throws UnsupportedCallbackException If the console is not available or other than
     *         PasswordCallback is supplied
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      Console cons = System.console();
      if (cons == null && password == null) {
        throw new UnsupportedCallbackException(callbacks[0], "Console is not available");
      }
      for (Callback callback : callbacks) {
        if (!(callback instanceof PasswordCallback)) {
          throw new UnsupportedCallbackException(callback);
        }
        PasswordCallback pwdCallback = (PasswordCallback) callback;
        if (password != null) {
          pwdCallback.setPassword(password);
          continue;
        }
        // It is used instead of cons.readPassword(prompt), because the prompt may contain '%'
        // characters
        pwdCallback.setPassword(cons.readPassword("%s", pwdCallback.getPrompt()));
      }
    }
  } // ConsoleCallbackHandler class
}
