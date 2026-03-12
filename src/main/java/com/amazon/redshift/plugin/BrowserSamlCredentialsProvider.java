package com.amazon.redshift.plugin;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.httpserver.RequestHandler;
import com.amazon.redshift.plugin.httpserver.Server;
import org.apache.http.NameValuePair;

import java.awt.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;

import static com.amazon.redshift.plugin.utils.CheckUtils.*;
import static com.amazon.redshift.plugin.utils.ResponseUtils.findParameter;

/**
 * Class to get SAML Assertion from a web service which is able to produce
 * SAML assertion by requesting the specific URL.
 */
public class BrowserSamlCredentialsProvider extends SamlCredentialsProvider
{
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
     * String containing "SAMLResponse" as a parameter key.
     */
    private static final String SAML_RESPONSE_PARAM_NAME = "SAMLResponse";

    /**
     * The value of parameter login_url
     */
    private String m_login_url;

    /**
     * The value of parameter idp_response_timeout in seconds.
     */
    private int m_idp_response_timeout = 120;

    /**
     * The value of parameter listen_port
     */
    private int m_listen_port = 7890;

    /**
     * Overridden method to grab the SAML Response. Used in base class to refresh temporary credentials.
     *
     * @return Base64 encoded SAML Response string
     * @throws IOException as part of common API. mean parameters are not set or has invalid values.
     */
    @Override
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
     * Overwritten method to grab the field parameters from JDBC connection string. This method calls the base class'
     * addParameter method and adds to it new specific parameters.
     *
     * @param key parameter key passed to JDBC
     * @param value parameter value associated with the given key
     */
    @Override
    public void addParameter(String key, String value)
    {
      	if (RedshiftLogger.isEnable())
      		m_log.logDebug("key: {0}", key);
    	
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
            default:
                super.addParameter(key, value);
        }
    }
    
    @Override
    public String getPluginSpecificCacheKey() {
    	return ((m_login_url != null) ? m_login_url : "")
    					;
    }
    

    /**
     * Authentication consists of:
     * <ol>
     * <li> Start the Socket Server on the port {@link BrowserSamlCredentialsProvider#m_listen_port}.</li>
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
            }, m_log);
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
              String.format("SSO URI: \n%s", authorizeRequestUrl.toString())
              );
        
        Desktop.getDesktop().browse(authorizeRequestUrl);
    }
}
