package com.amazon.redshift.plugin;

import com.amazon.redshift.INativePlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.core.IamHelper;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.httpserver.RequestHandler;
import com.amazon.redshift.plugin.httpserver.Server;
import com.amazon.redshift.util.RedshiftException;
import org.apache.commons.logging.LogFactory;
import org.apache.http.NameValuePair;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.function.Function;

import static com.amazon.redshift.plugin.utils.CheckUtils.*;
import static com.amazon.redshift.plugin.utils.ResponseUtils.findParameter;

public class BrowserOktaSAMLCredentialsProvider extends IdpCredentialsProvider implements INativePlugin {

    /**
     * The value of parameter login_url
     */
    private String m_login_url;

    /**
     * String containing "login_url" as a parameter key.
     */
    public static final String KEY_LOGIN_URL = "login_url";

    /**
     * String containing "idp_response_timeout" as a parameter key.
     */
    public static final String KEY_IDP_RESPONSE_TIMEOUT = "idp_response_timeout";

    /**
     * String containing "listen_port" as a parameter key.
     */
    public static final String KEY_LISTEN_PORT = "listen_port";

    /**
     * String containing "SAMLResponse" as a browser response Key
     */
    private static final String SAML_RESPONSE_PARAM_NAME = "SAMLResponse";

    /**
     * The value of parameter idp_response_timeout in seconds.
     */
    private int m_idp_response_timeout = 120;

    /**
     * The value of parameter listen_port
     */
    private int m_listen_port = 7890;

    /**
     * The value of expiry time
     */
    private int EXPIRY_TIME = 5;

    private static Map<String, NativeTokenHolder> m_cache = new HashMap<String, NativeTokenHolder>();
    private NativeTokenHolder m_lastRefreshCredentials; // Used when cache is disable.

    protected Boolean m_disableCache = false;

    /**
     * The custom log factory class.
     */
//    private static final Class<?> CUSTOM_LOG_FACTORY_CLASS = JwtCredentialsProvider.class;

    /**
     * Log properties file name.
     */
    private static final String LOG_PROPERTIES_FILE_NAME = "log-factory.properties";

    /**
     * Log properties file path.
     */
    private static final String LOG_PROPERTIES_FILE_PATH = "META-INF/services/org.apache.commons.logging.LogFactory";

    /**
     * A custom context class loader which allows us to control which LogFactory is loaded.
     * Our CUSTOM_LOG_FACTORY_CLASS will divert any wire logging to NoOpLogger to suppress wire
     * messages being logged.
     */
    private static final ClassLoader CONTEXT_CLASS_LOADER = new ClassLoader(
            BrowserOktaSAMLCredentialsProvider.class.getClassLoader())
    {
        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException
        {
            Class<?> clazz = getParent().loadClass(name);
            return clazz;
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException
        {
            if (LogFactory.FACTORY_PROPERTIES.equals(name))
            {
                // make sure not load any other commons-logging.properties files
                return Collections.enumeration(Collections.<URL>emptyList());
            }
            return super.getResources(name);
        }

        @Override
        public URL getResource(String name)
        {
            if (LOG_PROPERTIES_FILE_PATH.equals(name))
            {
                return BrowserOktaSAMLCredentialsProvider.class.getResource(LOG_PROPERTIES_FILE_NAME);
            }
            return super.getResource(name);
        }
    };


    @Override
    public void addParameter(String key, String value)
    {
        switch (key)
        {
            case KEY_LISTEN_PORT:
                m_listen_port = Integer.parseInt(value);

                if (RedshiftLogger.isEnable())
                   m_log.logDebug("m_listen_port: {0}", m_listen_port);

                break;
            case KEY_LOGIN_URL:
                m_login_url = value;

                if (RedshiftLogger.isEnable())
                    m_log.logDebug("m_login_url: {0}", m_login_url);

                break;
            case KEY_IDP_RESPONSE_TIMEOUT:
                m_idp_response_timeout = Integer.parseInt(value);

                if (RedshiftLogger.isEnable())
                   m_log.logDebug("m_idp_response_timeout: {0}", m_idp_response_timeout);

                break;
        }
    }

    @Override
    public void setLogger(RedshiftLogger log) {
        m_log = log;
    }

    @Override
    public String getPluginSpecificCacheKey() {
        return ((m_login_url != null) ? m_login_url : "");
    }

    @Override
    public String getIdpToken() throws RedshiftException {

        String saml = null;

        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            saml = getSamlAssertion();

            if (RedshiftLogger.isEnable())
                m_log.logDebug("BrowserOktaSAMLCredentialsProvider: got SAML token");
        }
        catch (Exception e)
        {
            if (RedshiftLogger.isEnable())
                m_log.logError(e);

            throw new RedshiftException("SAML error: " + e.getMessage(), e);
        }
        finally
        {
            currentThread.setContextClassLoader(cl);
        }

        return saml;

    }

    @Override
    public String getCacheKey() {
        String pluginSpecificKey = getPluginSpecificCacheKey();

        return  pluginSpecificKey;
    }

    @Override
    public NativeTokenHolder getCredentials() throws RedshiftException {
        NativeTokenHolder credentials = null;

        if(!m_disableCache)
        {
            String key = getCacheKey();
            credentials = m_cache.get(key);
        }

        if (credentials == null || credentials.isExpired())
        {
            if(RedshiftLogger.isEnable())
               m_log.logInfo("SAML getCredentials NOT from cache");

            synchronized(this) {
                refresh();

                if(m_disableCache) {
                    credentials = m_lastRefreshCredentials;
                    m_lastRefreshCredentials = null;
                }
            }
        }
        else {
            credentials.setRefresh(false);
            if(RedshiftLogger.isEnable())
                m_log.logInfo("SAML getCredentials from cache");
        }

        if(!m_disableCache) {
            credentials = m_cache.get(getCacheKey());
        }


        if (credentials == null)
        {
            throw new RedshiftException("Unable to get IDP credentials");
        }

        return credentials;
    }

    @Override
    public void refresh() throws RedshiftException {
        // Get the current thread and set the context loader with our custom load class method.
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();

        Thread.currentThread().setContextClassLoader(CONTEXT_CLASS_LOADER);

        try
        {
            String saml = getSamlAssertion();

            if (RedshiftLogger.isEnable())
                m_log.logDebug("BrowserOktaSAMLCredentialsProvider: refreshed SAML assertion token");

            // Default expiration until server sends actual expirations
            Date expiration = new Date(System.currentTimeMillis() + EXPIRY_TIME * 60 * 1000);
            NativeTokenHolder credentials = NativeTokenHolder.newInstance(saml, expiration);
            credentials.setRefresh(true);

            if(!m_disableCache)
                m_cache.put(getCacheKey(), credentials);
            else
                m_lastRefreshCredentials = credentials;
        }
        catch (Exception e)
        {
          if (RedshiftLogger.isEnable())
               m_log.logError(e);

            throw new RedshiftException("SAML error: " + e.getMessage(), e);
        }
        finally
        {
            currentThread.setContextClassLoader(cl);
        }
    }

    protected String getSamlAssertion() throws IOException
    {
        try
        {
            checkMissingAndThrows(m_login_url, KEY_LOGIN_URL);
            checkAndThrowsWithMessage(
                    m_idp_response_timeout < 10,
                    KEY_IDP_RESPONSE_TIMEOUT + " should be 10 seconds or greater.");
            checkInvalidAndThrows((m_listen_port < 1 || m_listen_port > 65535), KEY_LISTEN_PORT);
            validateURL(m_login_url);
            return authenticate();
        }
        catch (InternalPluginException ex)
        {
            // Wrap any exception to be compatible with SamlCredentialsProvider API
            throw new IOException(ex);
        }
    }
    /**
     * Authentication consists of:
     * <ol>
     * <li> Start the Socket Server on the port {@link BrowserOktaSAMLCredentialsProvider#m_listen_port}.</li>
     * <li> Open the default browser with the link asking a User to enter the credentials.</li>
     * <li> Retrieve the SAML Assertion string from the response.</li>
     * </ol>
     *
     * @return Base64 encoded SAML Assertion string
     * @throws IOException indicating the error
     */
    private String authenticate() throws IOException
    {
        RequestHandler requestHandler =
                new RequestHandler(new Function<List<NameValuePair>, Object>()
                {
                    @Override
                    public Object apply(List<NameValuePair> nameValuePairs)
                    {
                        if (RedshiftLogger.isEnable()) {
                            for (NameValuePair pair : nameValuePairs) {
                                if (pair.getName().equals(SAML_RESPONSE_PARAM_NAME)) {
                                    m_log.logDebug("nameValuePair:name= {0}", SAML_RESPONSE_PARAM_NAME);
                                } else {
                                    m_log.logDebug("nameValuePair: {0}", pair);
                                }
                            }
                        }

                        return findParameter(SAML_RESPONSE_PARAM_NAME, nameValuePairs);
                    }
                });
        Server server =
                new Server(m_listen_port, requestHandler, Duration.ofSeconds(m_idp_response_timeout), m_log);
        server.listen();

        if(RedshiftLogger.isEnable())
            m_log.log(LogLevel.DEBUG,
                    String.format("Listening for connection on port %d", m_listen_port));
        try
        {
            openBrowser();
            server.waitForResult();
        }

        catch (IOException ex)
        {
            if (RedshiftLogger.isEnable())
                m_log.logError(ex);

            server.stop();
            throw ex;
        }
        server.waitForResult();

        Object result = requestHandler.getResult();

        if (result instanceof InternalPluginException)
        {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Error occurred while fetching SAML assertion: {0}", result);
            throw (InternalPluginException) result;
        }
        if (result instanceof String)
        {
            if(RedshiftLogger.isEnable())
                m_log.log(LogLevel.DEBUG, "Got SAML assertion of length={0}", ((String) result).length());
            return (String) result;
        }

        if (RedshiftLogger.isEnable())
            m_log.logDebug("result: {0}", result);
        throw new InternalPluginException("Fail to login during timeout.");
    }

    /**
     * Opens the default browser with the authorization request to the web service.
     *
     * @throws IOException in case of error
     */
    private void openBrowser() throws IOException
    {
        URI authorizeRequestUrl = URI.create(m_login_url);

        if(RedshiftLogger.isEnable())
            m_log.log(LogLevel.DEBUG,
                    String.format("SSO URI: \n%s", authorizeRequestUrl)
            );
        validateURL(authorizeRequestUrl.toString());
        Desktop.getDesktop().browse(authorizeRequestUrl);
    }
}