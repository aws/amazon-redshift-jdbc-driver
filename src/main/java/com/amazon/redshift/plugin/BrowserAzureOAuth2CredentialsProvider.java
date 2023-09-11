package com.amazon.redshift.plugin;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.httpserver.RequestHandler;
import com.amazon.redshift.plugin.httpserver.Server;
import com.amazon.redshift.plugin.utils.RandomStateUtil;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static com.amazon.redshift.plugin.httpserver.RequestHandler.REDSHIFT_PATH;
import static com.amazon.redshift.plugin.utils.CheckUtils.*;
import static com.amazon.redshift.plugin.utils.ResponseUtils.findParameter;

/**
 * Class to get JWT Token from any IDP using OAuth 2.0 API
 */
public class BrowserAzureOAuth2CredentialsProvider extends JwtCredentialsProvider
{
    /**
     * Key for setting timeout for IDP response.
     */
    public static final String KEY_IDP_RESPONSE_TIMEOUT = "idp_response_timeout";

    /**
     * Key for setting the port number for listening.
     */
    public static final String KEY_LISTEN_PORT = "listen_port";

    /**
     * Key for setting idp tenant.
     */
    public static final String KEY_IDP_TENANT = "idp_tenant";

    /**
     * Key for setting client ID.
     */
    public static final String KEY_CLIENT_ID = "client_id";

    /**
     * Key for setting Scope.
     */
    public static final String KEY_SCOPE = "scope"; // "api://" + m_clientId + "/User.Read"
    
    
    /**
     * Key for setting state.
     */
    public static final String OAUTH_STATE_PARAMETER_NAME = "state";

    /**
     * Key for setting redirect URI.
     */
    public static final String OAUTH_REDIRECT_PARAMETER_NAME = "redirect_uri";

    /**
     * Key for setting code.
     */
    public static final String OAUTH_IDP_CODE_PARAMETER_NAME = "code";

    /**
     * Key for setting client ID.
     */
    public static final String OAUTH_CLIENT_ID_PARAMETER_NAME = "client_id";

    /**
     * Key for setting OAUTH response type.
     */
    public static final String OAUTH_RESPONSE_TYPE_PARAMETER_NAME = "response_type";

    /**
     * Key for setting requested token type.
     */
    public static final String OAUTH_REQUESTED_TOKEN_TYPE_PARAMETER_NAME = "requested_token_type";

    /**
     * Key for setting grant type.
     */
    public static final String OAUTH_GRANT_TYPE_PARAMETER_NAME = "grant_type";

    /**
     * Key for setting scope.
     */
    public static final String OAUTH_SCOPE_PARAMETER_NAME = "scope";

    /**
     * Key for setting resource.
     */
    public static final String OAUTH_RESOURCE_PARAMETER_NAME = "resource";

    /**
     * Key for setting response mode.
     */
    public static final String OAUTH_RESPONSE_MODE_PARAMETER_NAME = "response_mode";

    /**
     * String containing Microsoft IDP host.
     */
    private static final String MICROSOFT_IDP_HOST = "login.microsoftonline.com";

    /**
     * String containing HTTPS.
     */
    private static final String CURRENT_INTERACTION_SCHEMA = "https";

    /**
     * IDP tenant variable.
     */
    private String m_idp_tenant;

    /**
     * Client ID variable.
     */
    private String m_clientId;

    /**
     * Application Scope variable.
     */
    private String m_scope = "";
    
    /**
     * Default timeout for IDP response.
     */
    private int m_idp_response_timeout = 120;

    /**
     *  Default port for local server.
     */
    private int m_listen_port = 0;

    /**
     * Redirect URI variable.
     */
    private String redirectUri;

    private void checkRequiredParameters() throws IOException
    {
      checkMissingAndThrows(m_idp_tenant, KEY_IDP_TENANT);
      checkMissingAndThrows(m_clientId, KEY_CLIENT_ID);
      checkAndThrowsWithMessage(
          m_idp_response_timeout < 10,
          KEY_IDP_RESPONSE_TIMEOUT + " should be 10 seconds or greater.");
      checkInvalidAndThrows( m_listen_port != 0 && (  m_listen_port < 1 || m_listen_port > 65535), KEY_LISTEN_PORT);
      
    }
    
    /**
     * Overridden method to grab the JWT Response. Used in base class to refresh temporary credentials.
     *
     * @return Base64 encoded JWT Response string
     * @throws IOException indicating the error
     */
    @Override
    protected String getJwtAssertion() throws IOException
    {
        try
        {
            checkRequiredParameters();
            if( m_listen_port == 0 )
            {
                m_log.logDebug("Listen port set to 0. Will pick random port");
            }

            String token = fetchAuthorizationToken();
            String content = fetchJwtResponse(token);
            String jwtAssertion = extractJwtAssertion(content);
            return jwtAssertion;
        }
        catch (InternalPluginException | URISyntaxException ex)
        {
        	if (RedshiftLogger.isEnable())
        		m_log.logError(ex);
        	
            // Wrap any exception to be compatible with JwtCredentialsProvider API
            throw new IOException(ex);
        }
    }

    /**
     * Overwritten method to grab the field parameters from JDBC connection string. This method calls the base class'
     * addParameter method and adds to it new specific parameters.
     *
     * @param key   parameter key passed to JDBC
     * @param value parameter value associated with the given key
     */
    @Override
    public void addParameter(String key, String value)
    {
    	if (RedshiftLogger.isEnable())
    		m_log.logDebug("key: {0}", key);
    	
        switch (key)
        {
            case KEY_IDP_TENANT:
                m_idp_tenant = value;
                
              	if (RedshiftLogger.isEnable())
              		m_log.logDebug("m_idp_tenant: {0}", m_idp_tenant);
              	
                break;
            case KEY_CLIENT_ID:
            	
                m_clientId = value;
                
              	if (RedshiftLogger.isEnable())
              		m_log.logDebug("m_clientId: {0}", m_clientId);
                
                break;
            case KEY_SCOPE:
              
              m_scope = value;
              
              if (RedshiftLogger.isEnable())
                  m_log.logDebug("m_scope: {0}", m_scope);
              
              break;
            case KEY_IDP_RESPONSE_TIMEOUT:
                m_idp_response_timeout = Integer.parseInt(value);
                
              	if (RedshiftLogger.isEnable())
              		m_log.logDebug("m_idp_response_timeout: {0}", m_idp_response_timeout);
                
                break;
            case KEY_LISTEN_PORT:
                m_listen_port = Integer.parseInt(value);
                
              	if (RedshiftLogger.isEnable())
              		m_log.logDebug("m_listen_port: {0}", m_listen_port);
              	
                break;
            default:
                super.addParameter(key, value);
        }
    }
    
    @Override
    public String getPluginSpecificCacheKey() {
    	return ((m_idp_tenant != null) ? m_idp_tenant : "")
    					+ ((m_clientId != null) ? m_clientId : "")
    					;
    }
    

    /**
     * First authentication phase:
     * <ol>
     * <li> Set the state in order to check if the incoming request belongs to the current authentication process.</li>
     * <li> Start the Socket Server at the {@linkplain BrowserAzureOAuth2CredentialsProvider#m_listen_port} port.</li>
     * <li> Open the default browser with the link asking a User to enter the credentials.</li>
     * <li> Retrieve the JWT Assertion string from the response. Decode it, format, validate and return.</li>
     * </ol>
     *
     * @return Authorization token
     */
    private String fetchAuthorizationToken() throws IOException, URISyntaxException
    {
        final String state = RandomStateUtil.generateRandomState();
        RequestHandler requestHandler =
            new RequestHandler(new Function<List<NameValuePair>, Object>()
            {
                @Override
                public Object apply(List<NameValuePair> nameValuePairs)
                {
                    String incomingState =
                        findParameter(OAUTH_STATE_PARAMETER_NAME, nameValuePairs);
                    if (!state.equals(incomingState))
                    {
                        return new InternalPluginException(
                            "Incoming state " + incomingState +
                                " does not match the outgoing state " + state);
                    }
                    String code = findParameter(OAUTH_IDP_CODE_PARAMETER_NAME, nameValuePairs);
                    if (isNullOrEmpty(code))
                    {
                        return new InternalPluginException("No valid code found");
                    }
                    return code;
                }
            });
        Server server =
            new Server(m_listen_port, requestHandler, Duration.ofSeconds(m_idp_response_timeout), m_log);
        server.listen();
        int localPort = server.getLocalPort();
        this.redirectUri = "http://localhost:" + localPort + REDSHIFT_PATH;
        try
        {
      		if(RedshiftLogger.isEnable())
      			m_log.log(LogLevel.DEBUG,
              String.format("Listening for connection on port %d", m_listen_port));
            openBrowser(state);
            server.waitForResult();
        }
        catch (URISyntaxException | IOException ex)
        {
        	if (RedshiftLogger.isEnable())
        		m_log.logError(ex);
        	
            server.stop();
            throw ex;
        }
        
        Object result = requestHandler.getResult();
        
        if (result instanceof InternalPluginException)
        {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Error occurred while fetching JWT assertion: {0}", result);
            throw (InternalPluginException) result;
        }
        if (result instanceof String)
        {
        	if(RedshiftLogger.isEnable())
        		m_log.log(LogLevel.DEBUG, "Got authorization token of length={0}", ((String) result).length());
          return (String) result;
        }

        if (RedshiftLogger.isEnable())
            m_log.logDebug("result: {0}", result);
        throw new InternalPluginException("Fail to login during timeout.");
    }

    /**
     * Initiates the request to the IDP and gets the response body
     *
     * @param token authorization token
     * @return Response body of the incoming response
     * @throws IOException indicating the error
     */
    private String fetchJwtResponse(String token) throws IOException
    {
        HttpPost post = createAuthorizationRequest(token);
        try (
            CloseableHttpClient client = getHttpClient();
            CloseableHttpResponse resp = client.execute(post))
        {
        	String content = EntityUtils.toString(resp.getEntity());

            if(RedshiftLogger.isEnable()) {
                String maskedContent = content.replaceAll(getRegexForJsonKey("access_token"), "$1***masked***\"");
                maskedContent = maskedContent.replaceAll(getRegexForJsonKey("id_token"), "$1***masked***\"");
                m_log.log(LogLevel.DEBUG, "fetchJwtResponse https response:" + maskedContent);
            }
        	
          checkAndThrowsWithMessage(
              resp.getStatusLine().getStatusCode() != 200,
              "Unexpected response:  " + resp.getStatusLine().getReasonPhrase());
            
          return content;
        }
        catch (GeneralSecurityException ex)
        {
        	if(RedshiftLogger.isEnable())
        		m_log.log(LogLevel.ERROR,ex.getMessage(),ex);
        	
          throw new InternalPluginException(ex);
        }
    }

    /**
     * Get Base 64 encoded JWT assertion from the response body
     *
     * @param content response body
     * @return string containing Base 64 encoded JWT assertion
     */
    private String extractJwtAssertion(String content)
    {
        String encodedJwtAssertion;
        
        JsonNode accessTokenField = Jackson.jsonNodeOf(content).findValue("access_token");
        checkAndThrowsWithMessage(accessTokenField == null, "Failed to find access_token");
        encodedJwtAssertion = accessTokenField.textValue();
        checkAndThrowsWithMessage(
            isNullOrEmpty(encodedJwtAssertion),
            "Invalid access_token value.");
        
      	if(RedshiftLogger.isEnable())
      		m_log.log(LogLevel.DEBUG, "Successfully got JWT assertion");
      	
        return encodedJwtAssertion;
    }

    /**
     * Populates request URI and parameters.
     *
     * @param authorizationCode authorization authorizationCode
     * @return object containing the request data
     * @throws IOException 
     */
    private HttpPost createAuthorizationRequest(String authorizationCode) throws IOException
    {
        URIBuilder builder = new URIBuilder().setScheme(CURRENT_INTERACTION_SCHEMA)
            .setHost(MICROSOFT_IDP_HOST)
            .setPath("/" + m_idp_tenant + "/oauth2/v2.0/token");

        String tokenRequestUrl = builder.toString();
        String scope = "openid " + m_scope;
        
        validateURL(tokenRequestUrl);        
        HttpPost post = new HttpPost(tokenRequestUrl);
        final List<BasicNameValuePair> parameters = new ArrayList<>();
        parameters.add(new BasicNameValuePair(OAUTH_IDP_CODE_PARAMETER_NAME, authorizationCode));
/*        parameters.add(
            new BasicNameValuePair(
                OAUTH_REQUESTED_TOKEN_TYPE_PARAMETER_NAME,
                "urn:ietf:params:oauth:token-type:jwt"));
*/                
        parameters.add(new BasicNameValuePair(OAUTH_RESPONSE_TYPE_PARAMETER_NAME, "token"));
                
        parameters
            .add(new BasicNameValuePair(OAUTH_GRANT_TYPE_PARAMETER_NAME, "authorization_code"));
        parameters.add(new BasicNameValuePair(OAUTH_SCOPE_PARAMETER_NAME, scope));
//        parameters.add(new BasicNameValuePair(OAUTH_RESOURCE_PARAMETER_NAME, m_clientId));
        parameters.add(new BasicNameValuePair(OAUTH_CLIENT_ID_PARAMETER_NAME, m_clientId));
        parameters.add(new BasicNameValuePair(OAUTH_REDIRECT_PARAMETER_NAME, redirectUri));

        post.addHeader(
            HttpHeaders.CONTENT_TYPE,
            ContentType.APPLICATION_FORM_URLENCODED.toString());
        post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString());
        post.setEntity(new UrlEncodedFormEntity(parameters, StandardCharsets.UTF_8));
        
      	if(RedshiftLogger.isEnable())
      		m_log.log(LogLevel.DEBUG,
            String.format(
                "Request token URI: \n%s\nredirectUri:%s",
                tokenRequestUrl, redirectUri)
            );
      	
        return post;
    }

    /**
     * Opens the default browser with the authorization request to the IDP
     *
     * @param state
     * @throws IOException indicating the error
     */
    private void openBrowser(String state) throws URISyntaxException, IOException
    {
        String scope = "openid " + m_scope;
        URIBuilder builder = new URIBuilder().setScheme(CURRENT_INTERACTION_SCHEMA)
            .setHost(MICROSOFT_IDP_HOST)
            .setPath("/" + m_idp_tenant + "/oauth2/v2.0/authorize")
            .addParameter(OAUTH_SCOPE_PARAMETER_NAME, scope)
            .addParameter(OAUTH_RESPONSE_TYPE_PARAMETER_NAME, "code")
            .addParameter(OAUTH_RESPONSE_MODE_PARAMETER_NAME, "form_post")
            .addParameter(OAUTH_CLIENT_ID_PARAMETER_NAME, m_clientId)
            .addParameter(OAUTH_REDIRECT_PARAMETER_NAME, redirectUri)
            .addParameter(OAUTH_STATE_PARAMETER_NAME, state);
        URI authorizeRequestUrl;
        authorizeRequestUrl = builder.build();
        
        validateURL(authorizeRequestUrl.toString());           
        
        Desktop.getDesktop().browse(authorizeRequestUrl);
        
      	if(RedshiftLogger.isEnable())
	        m_log.log(LogLevel.DEBUG,
	            String.format("Authorization code request URI: \n%s", authorizeRequestUrl.toString()));
    }
}
