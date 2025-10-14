/**
* Copyright 2010-2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
* <p>
* This file is licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License. A copy of
* the License is located at
* <p>
* http://aws.amazon.com/apache2.0/
* <p>
* This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/

package com.amazon.redshift.plugin;

import com.amazon.redshift.core.Utils;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.httpserver.RequestHandler;
import com.amazon.redshift.plugin.httpserver.Server;
import com.amazon.redshift.plugin.utils.RandomStateUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.security.SecureRandom;
import java.util.Base64;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssooidc.SsoOidcClient;
import software.amazon.awssdk.services.ssooidc.model.AccessDeniedException;
import software.amazon.awssdk.services.ssooidc.model.AuthorizationPendingException;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenRequest;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenResponse;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientRequest;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientResponse;
import software.amazon.awssdk.services.ssooidc.model.SlowDownException;
import software.amazon.awssdk.services.ssooidc.model.SsoOidcException;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import static com.amazon.redshift.plugin.utils.ResponseUtils.findParameter;

public class BrowserIdcAuthPlugin extends CommonCredentialsProvider {
	/**
     * Key for setting timeout for IDP response.
     */
    public static final String KEY_IDC_RESPONSE_TIMEOUT = "idp_response_timeout";

    /**
     * Key for setting the port number for listening.
     */
    public static final String KEY_LISTEN_PORT = "listen_port";

    /**
     * Key for setting idp tenant.
     */
    public static final String KEY_ISSUER_URL = "issuer_url";

    /**
     * Key for setting IdC region.
     */
    public static final String KEY_IDC_REGION = "idc_region";

	/**
	 * Key for setting IdC client display name
	 */
	private static final String KEY_IDC_CLIENT_DISPLAY_NAME = "idc_client_display_name";

    /**
     * Key for setting CSRF endpoint protection state.
     */
    public static final String OAUTH_CSRF_STATE_PARAMETER_NAME = "state";

    /**
     * Key for setting redirect URI.
     */
    public static final String OAUTH_REDIRECT_PARAMETER_NAME = "redirect_uri";

    /**
     * Key for setting client ID.
     */
    public static final String OAUTH_CLIENT_ID_PARAMETER_NAME = "client_id";

    /**
     * Key for setting OAUTH response type.
     */
    public static final String OAUTH_RESPONSE_TYPE_PARAMETER_NAME = "response_type";

    /**
     * Key for setting grant type.
     */
    public static final String OAUTH_GRANT_TYPE_PARAMETER_NAME = "grant_type";

    /**
     * Key for setting scope.
     */
    public static final String OAUTH_SCOPE_PARAMETER_NAME = "scopes";

    /**
     * Key for setting code challenge.
     */
    public static final String OAUTH_CODE_CHALLENGE_PARAMETER_NAME = "code_challenge";

    /**
     * Key for setting code challenge.
     */
    public static final String OAUTH_CHALLENGE_METHOD_PARAMETER_NAME = "code_challenge_method";

	/**
	 * The default time in seconds for which the client must wait between attempts when polling for a session
	 */
	public final int CREATE_TOKEN_POLLING_INTERVAL = 1;

	/**
	 * It is used if auth server doesn't provide any value access token expiration
	 */
	public final int DEFAULT_IDC_TOKEN_EXPIRY_IN_SEC = 900;

	/**
	 * It is used to set the number of bytes of the code verifier
	 */
	public final int CODE_VERIFIER_BYTE_LENGTH = 60;

	/**
	 * It is used to multiply millisecond values to get seconds
	 */
	public final long MILLISECOND_MULTIPLIER = 1000L;

	/**
	 * Issuer URL variable.
	 */
    protected String m_issuer_url;

    /**
	 * IdC region variable.
	 */
    protected String m_idc_region;

	/**
	 * Redirect URI variable.
	 */
    protected String m_redirect_uri;

	/**
	 * AWSSSOOIDC client object needed to SSOOIDC methods
	 */
	protected SsoOidcClient m_sdk_client;

	/**
	 * Key for authorization code.
	 */
    private static final String AUTH_CODE_PARAMETER_NAME = "code";

    /**
	 * String containing HTTPS.
	 */
    private static final String CURRENT_INTERACTION_PROTOCOL = "https";

	/**
	 * String containing OIDC used for building the authorization server endpoint.
	 */
    private static final String OIDC_SUBDOMAIN = "oidc";

	/**
	 * String containing amazonaws.com used for building the authorization server endpoint.
	 */
    private static final String AMAZON_COM_DOMAIN = "amazonaws.com";

	/**
	 * Application scope variable.
	 */
    private static final String REDSHIFT_IDC_CONNECT_SCOPE = "redshift:connect";

	/**
	 * Application grant types variable.
	 */
    private static final String AUTH_CODE_GRANT_TYPE = "authorization_code";

	/**
	 * Client type of client application
	 */
	private static final String M_CLIENT_TYPE = "public";

	/**
	 * Redirect URI of client application
	 */
	private static final String M_REDIRECT_URI = "http://127.0.0.1";

	/**
	 * Authorize endpoint to get authorization code
	 */
	private static final String AUTHORIZE_ENDPOINT = "/authorize";

	/**
	 * Method used to hash the code verifier
	 */
	private static final String CHALLENGE_METHOD = "S256";

	/**
	 * SHA256 hash used to hash the code verifier
	 */
	private static final String SHA256_METHOD = "SHA-256";
    /**
     * Default timeout for IDP response.
     */
    private int m_idc_response_timeout = 120;

    /**
     *  Default port for local server.
     */
    private int m_listen_port = 7890;

	/**
	 *  Default IdC client display name.
	 */
	private String m_idcClientDisplayName = RedshiftProperty.IDC_CLIENT_DISPLAY_NAME.getDefaultValue();

	// Used to cache RegisterClientResponse, which contains clientId and clientSecret. Cache key will be <m_issuer_url>:<m_idc_region>:<m_listen_port>
	private static final Map<String, RegisterClientResponse> m_register_client_cache = new HashMap<String, RegisterClientResponse>();
    
	/**
	 * Overridden method to obtain the auth token from plugin specific implementation
	 *
	 * @return {@link NativeTokenHolder} A wrapper containing auth token and its expiration time information
	 * @throws IOException indicating the error
	*/
	@Override
	protected NativeTokenHolder getAuthToken() throws IOException {
		return getIdcToken();
	}

	/**
	 * Returns the retrieved access token from IdC authorization server
	 *
	 * @return {@link NativeTokenHolder} This contains the retrieved access token and the expiration time of that token
	 * @throws IOException if an error occurs during the involved API call
	 */
	protected NativeTokenHolder getIdcToken() throws IOException {
		try {
			checkRequiredParameters();
			m_sdk_client = SsoOidcClient.builder()
					.region(Region.of(m_idc_region))
					.build();
			m_redirect_uri = M_REDIRECT_URI + ":" + m_listen_port;

			RegisterClientResponse registerClientResponse = getRegisterClientResponse();

			String codeVerifier = generateCodeVerifier();

			String codeChallenge = generateCodeChallenge(codeVerifier);

			String authCode = fetchAuthorizationCode(codeChallenge, registerClientResponse);

			CreateTokenResponse createTokenResponse = fetchTokenResponse(registerClientResponse, authCode,codeVerifier);

			return processCreateTokenResponse(createTokenResponse);
		} catch (InternalPluginException | URISyntaxException ex) {
			if (RedshiftLogger.isEnable())
				m_log.log(LogLevel.ERROR, ex, "InternalPluginException in getIdcToken");
			// Wrap any exception to be compatible with CommonCredentialsProvider API
			throw new IOException(ex.getMessage(), ex);
		}
	}

	private void checkRequiredParameters() throws InternalPluginException {
		if (Utils.isNullOrEmpty(m_issuer_url)) {
			m_log.logDebug("IdC authentication failed: issuer_url needs to be provided in connection params");
			throw new InternalPluginException("IdC authentication failed: The issuer URL must be included in the connection parameters.");
		}
		if (Utils.isNullOrEmpty(m_idc_region)) {
			m_log.logDebug("IdC authentication failed: idc_region needs to be provided in connection params");
			throw new InternalPluginException("IdC authentication failed: The IdC region must be included in the connection parameters.");
		}
	}

	/**
	* Overridden method to grab the field parameters from JDBC connection string or extended params provided by user.
	* This method calls the base class' addParameter method and adds to it new specific parameters.
	*
	* @param key   parameter key passed to JDBC driver
	* @param value parameter value associated with the given key
	*/
	@Override
	public void addParameter(String key, String value) {
		switch (key) {
			case KEY_ISSUER_URL:
				m_issuer_url = value;
				if (RedshiftLogger.isEnable())
					m_log.logDebug("Setting issuer_url: {0}", m_issuer_url);
				break;

			case KEY_IDC_REGION:
				m_idc_region = value;
				if (RedshiftLogger.isEnable())
					m_log.logDebug("Setting idc_region: {0}", m_idc_region);
				break;

			case KEY_LISTEN_PORT:
				m_listen_port = Integer.parseInt(value);
				if (RedshiftLogger.isEnable())
					m_log.logDebug("Setting listen_port: {0}", m_listen_port);
				break;

			case KEY_IDC_CLIENT_DISPLAY_NAME:
				if (!Utils.isNullOrEmpty(value)) {
					m_idcClientDisplayName = value;
				}
				if (RedshiftLogger.isEnable())
					m_log.logDebug("Setting idc_client_display_name: {0}", m_idcClientDisplayName);
				break;

			case KEY_IDC_RESPONSE_TIMEOUT:
				if (!Utils.isNullOrEmpty(value)) {
					int timeout = Integer.parseInt(value);
					if (timeout > 10) { // minimum allowed timeout value is 10 secs
						m_idc_response_timeout = timeout;
						if (RedshiftLogger.isEnable())
							m_log.logDebug("Setting idc_response_timeout={0}", m_idc_response_timeout);
					} else { // else use default timeout value itself
						if (RedshiftLogger.isEnable())
							m_log.logDebug("Setting default idc_response_timeout={0}; provided value={1}", m_idc_response_timeout, timeout);
					}
				}
				break;

			default:
				super.addParameter(key, value);
		}
	}

	/**
	 * Registers a client with IAM Identity Center. This allows clients to initiate authorization code + PKCE flow.
	 * The output is persisted for reuse through many authentication requests.
	 *
	 * @return {@link RegisterClientResponse} Client registration response containing {@code clientId} and {@code clientSecret} required for authorization code + PKCE flow
	 * @throws IOException if an error occurs during the involved API call
	 */
	protected RegisterClientResponse getRegisterClientResponse() throws IOException {
		String registerClientCacheKey = m_issuer_url + ":" + m_idc_region + ":" +  m_listen_port;
		RegisterClientResponse cachedRegisterClientResponse = m_register_client_cache.get(registerClientCacheKey);
		if (isCachedRegisterClientResponseValid(cachedRegisterClientResponse)) {
			if (RedshiftLogger.isEnable()){
				m_log.logDebug("Using cached register client response");
				m_log.logDebug("Cached register client secret expiry is {0}", cachedRegisterClientResponse.clientSecretExpiresAt());
			}
			return cachedRegisterClientResponse;
		}

		RegisterClientRequest registerClientRequest = RegisterClientRequest.builder()
				.clientName(m_idcClientDisplayName)
				.clientType(M_CLIENT_TYPE)
				.scopes(REDSHIFT_IDC_CONNECT_SCOPE)
				.issuerUrl(m_issuer_url)
				.redirectUris(m_redirect_uri)
				.grantTypes(AUTH_CODE_GRANT_TYPE)
				.build();

		RegisterClientResponse registerClientResponse = null;
		try {
			registerClientResponse = m_sdk_client.registerClient(registerClientRequest);
			if (RedshiftLogger.isEnable() && registerClientResponse.sdkHttpResponse() != null) {
				m_log.logDebug("registerClient response code: {0}", registerClientResponse.sdkHttpResponse().statusCode());
			}
		} catch (SsoOidcException ex) {
			if (RedshiftLogger.isEnable())
				m_log.log(LogLevel.ERROR, ex, "Error: Unexpected server error while registering client;");
			throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
		} catch (Exception ex) {
			if (RedshiftLogger.isEnable())
				m_log.log(LogLevel.ERROR, ex, "Error: Unexpected register client error;");
			throw new IOException("IdC registerClient failed : There was an error during the request.", ex);
		}
		m_register_client_cache.put(registerClientCacheKey, registerClientResponse);
		if (RedshiftLogger.isEnable())
			m_log.logDebug("Cached register client secret expiry is {0}", registerClientResponse.clientSecretExpiresAt());
		
		return registerClientResponse;
	}

	/**
	 * Generates a high entropy random codeVerifier string that will be used in the PKCE flow
	 *
	 * @return codeVerifier: randomly generated base64 encoded 60 byte string
	 */
	protected String generateCodeVerifier() {
        byte[] randomBytes = new byte[CODE_VERIFIER_BYTE_LENGTH];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(randomBytes);

		String codeVerifier = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);

        return codeVerifier;
    }

	/**
	 * Applies a SHA256 hash to the code verifier
	 *
	 * @param verifier Randomly generated base64 encoded string
	 * @return codeChallenge: string generated from applying a SHA256 hash to the code verifier
	 */
	protected String generateCodeChallenge(String verifier) {
        byte[] sha256Hash = sha256(verifier.getBytes(StandardCharsets.US_ASCII));

        // Encode the hash into Base64url
        String codeChallenge = Base64.getUrlEncoder().withoutPadding().encodeToString(sha256Hash);

        return codeChallenge;
    }

	/**
	 * Retrieves an IdC vended authorization code from the IdC server through the redirectURI
	 * 
	 * @param codeChallenge String generated from applying a SHA256 hash to the code verifier
	 * @param registerClientResponse Contains the clientId and clientSecret
	 * @return {@link String} authCode: Authorization code returned from the IdC authorization server used to get the access token
	 * @throws IOException If an I/O error occurs while communicating with the IdC server or processing the response
	 * @throws URISyntaxException If the redirect URI or any other URI involved in the authorization process is malformed or violates URI syntax rules
	 */
	protected String fetchAuthorizationCode(String codeChallenge, RegisterClientResponse registerClientResponse) throws IOException, URISyntaxException
    {
        final String state = RandomStateUtil.generateRandomState();
		RequestHandler requestHandler =
            new RequestHandler(new Function<List<NameValuePair>, Object>()
            {
                @Override
                public Object apply(List<NameValuePair> nameValuePairs)
                {
                    String incomingState =
                        findParameter(OAUTH_CSRF_STATE_PARAMETER_NAME, nameValuePairs);
                    if (!state.equals(incomingState))
                    {
						String state_error_message = "Incoming state " + incomingState + " does not match the outgoing state " + state;
						m_log.log(LogLevel.DEBUG, state_error_message);
                        return new InternalPluginException(state_error_message);
                    }
                    String code = findParameter(AUTH_CODE_PARAMETER_NAME, nameValuePairs);
                    if (Utils.isNullOrEmpty(code))
                    {
						String code_error_message = "No valid code found";
						m_log.log(LogLevel.DEBUG, code_error_message);
                        return new InternalPluginException(code_error_message);
                    }
                    return code;
                }
            });

        Server server = new Server(m_listen_port, requestHandler, Duration.ofSeconds(m_idc_response_timeout), m_log);

        try
        {
            server.listen();
            if(RedshiftLogger.isEnable())
                m_log.log(LogLevel.DEBUG, String.format("Listening for connection on port %d", m_listen_port));
            openBrowser(state, codeChallenge, registerClientResponse);
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
                m_log.logDebug("Error occurred while fetching authorization code: {0}", result);
            throw (InternalPluginException) result;
        }
        if (result instanceof String)
        {
        	if(RedshiftLogger.isEnable())
        		m_log.log(LogLevel.DEBUG, "Got authorization code of length={0}", ((String) result).length());
          return (String) result;
        }

        if (RedshiftLogger.isEnable())
            m_log.logDebug("result: {0}", result);
        throw new InternalPluginException("Error fetching authentication code from browser. Failed to login during timeout.");
    }

	/**
	 * Creates and returns an access token for the authorized client within if successful before the timeout
	 * 
	 * @param registerClientResponse Contains the clientId and clientSecret
	 * @param authCode Authorization code returned from the IdC authorization server used to get the access token
	 * @param codeVerifier Randomly generated base64 encoded 60 byte string
	 * @return {@link CreateTokenResponse} Create token response containing IdC token
	 * @throws IOException If an I/O error occurs while communicating with the IdC server or processing the response
	 */
	protected CreateTokenResponse fetchTokenResponse(RegisterClientResponse registerClientResponse, String authCode, String codeVerifier) throws IOException {
		long pollingEndTime = System.currentTimeMillis() + m_idc_response_timeout * MILLISECOND_MULTIPLIER;

		int pollingIntervalInSec = CREATE_TOKEN_POLLING_INTERVAL;

		// poll for create token with pollingIntervalInSec wait time between each attempt until pollingEndTime
		while (System.currentTimeMillis() < pollingEndTime) {
			try {
				CreateTokenResponse createTokenResponse = getCreateTokenResponse(registerClientResponse.clientId(), registerClientResponse.clientSecret(), authCode, AUTH_CODE_GRANT_TYPE, codeVerifier, m_redirect_uri);
				if (RedshiftLogger.isEnable() && registerClientResponse.sdkHttpResponse() != null) {
					m_log.logDebug("createToken response code: {0}", createTokenResponse.sdkHttpResponse().statusCode());
				}
				if (createTokenResponse != null && createTokenResponse.accessToken() != null) {
					return createTokenResponse;
				} else {
					// auth server sent a non exception response without valid token, so throw error
					if (RedshiftLogger.isEnable())
						m_log.logError("Failed to fetch an IdC access token");
					throw new IOException("IdC authentication failed : The credential token couldn't be fetched.");
				}
			} catch (AuthorizationPendingException ex) {
				if (RedshiftLogger.isEnable())
					m_log.logDebug("Browser authorization pending from user");
			} catch (SlowDownException ex) {
				if (RedshiftLogger.isEnable())
					m_log.log(LogLevel.ERROR, ex, "Error: Too frequent createToken requests made by client;");
				throw new IOException("IdC authentication failed : Requests to the IdC service are too frequent.", ex);
			} catch (AccessDeniedException ex) {
				if (RedshiftLogger.isEnable())
					m_log.log(LogLevel.ERROR, ex, "Error: Access denied, please ensure app assignment is done for the user;");
				throw new IOException("IdC authentication failed : You don't have sufficient permission to perform the action. Please ensure app assignment is done for the user.", ex);
			} catch (SsoOidcException ex) {
				if (RedshiftLogger.isEnable())
					m_log.log(LogLevel.ERROR, ex, "Error: Server error in creating token;");
				throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
			} catch (Exception ex) {
				if (RedshiftLogger.isEnable())
					m_log.log(LogLevel.ERROR, ex, "Error: Unexpected error in create token;");
				throw new IOException("IdC createToken failed : There was an error during the request.", ex);
			}

			try {
				Thread.sleep(pollingIntervalInSec * MILLISECOND_MULTIPLIER);
			} catch (InterruptedException ex) {
				if (RedshiftLogger.isEnable())
					m_log.log(LogLevel.ERROR, ex, "Thread interrupted during sleep");
			}
		}

		if (RedshiftLogger.isEnable())
			m_log.logError("Error: Request timed out while waiting for user authentication in the browser");
		throw new IOException("IdC authentication failed : The request timed out. Authentication wasn't completed.");
	}

	/**
	 * Creates and returns an access token for the authorized client.
	 * The access token issued will be used by the Redshift server to authorize the user.
	 *
	 * @param clientId     The unique identifier string for each client
	 * @param clientSecret A secret string generated for the client
	 * @param authCode   Authorization code used to get the access token
	 * @param grantType    Supports grant types for the authorization code request
	 * @param codeVerifier    Used for PKCE flow
	 * @param redirectUri    Used to verify that the redirectUri is the same
	 * @return {@link CreateTokenResponse} Create token response containing IdC token
	 */
	protected CreateTokenResponse getCreateTokenResponse(String clientId, String clientSecret, String authCode, String grantType, String codeVerifier, String redirectUri) {
		CreateTokenRequest createTokenRequest = CreateTokenRequest.builder()
				.clientId(clientId)
				.clientSecret(clientSecret)
				.code(authCode)
				.grantType(grantType)
				.codeVerifier(codeVerifier)
				.redirectUri(redirectUri)
				.build();

		return m_sdk_client.createToken(createTokenRequest);
	}

	/**
	 * Takes a created token response as input and returns an object of type NativeTokenHolder that contains the access token and expiration
	 * 
	 * @param createTokenResponse Contains the access token, refresh token, and access token expiry
	 * @return {@link NativeTokenHolder} This contains the retrieved access token and the expiration time of that token
	 * 
	 * @throws IOException If an I/O error occurs while communicating with the IdC server or processing the response
	 */
	protected NativeTokenHolder processCreateTokenResponse(CreateTokenResponse createTokenResponse) throws IOException {
		String idcToken = createTokenResponse.accessToken();
		if(Utils.isNullOrEmpty(idcToken)) {
			throw new InternalPluginException("Returned access token is null or empty.");
		}
		int expiresInSecs = DEFAULT_IDC_TOKEN_EXPIRY_IN_SEC;
		if (createTokenResponse.expiresIn() != null && createTokenResponse.expiresIn() > 0) {
			expiresInSecs = createTokenResponse.expiresIn();
		}

		Date expiration = new Date(System.currentTimeMillis() + expiresInSecs * MILLISECOND_MULTIPLIER);
		if (RedshiftLogger.isEnable())
			m_log.logDebug("Access token expires at {0}", expiration);

		return NativeTokenHolder.newInstance(idcToken, expiration);
	}

	/**
	 * Opens the default browser with the authorization code request to IdC /authorize endpoint
	 *
	 * @param state A randomly generated string to protect against cross-site request forgery attacks
	 * @param codeChallenge String generated from applying a SHA256 hash to the code verifier
	 * @param registerClientResponse Contains the clientId and clientSecret
	 *
	 * @throws IOException If an error occurs while opening the default browser or establishing a connection to the IdC /authorize endpoint
	 * @throws URISyntaxException If the URI used for the authorization code request is malformed or violates URI syntax rules
	*/
    protected void openBrowser(String state, String codeChallenge, RegisterClientResponse registerClientResponse) throws URISyntaxException, IOException
    {
		String idc_host = createIdcHost(m_idc_region);
        URIBuilder builder = new URIBuilder().setScheme(CURRENT_INTERACTION_PROTOCOL)
            .setHost(idc_host)
            .setPath(AUTHORIZE_ENDPOINT)
            .addParameter(OAUTH_RESPONSE_TYPE_PARAMETER_NAME, AUTH_CODE_PARAMETER_NAME)
            .addParameter(OAUTH_CLIENT_ID_PARAMETER_NAME, registerClientResponse.clientId())
            .addParameter(OAUTH_REDIRECT_PARAMETER_NAME, m_redirect_uri)
            .addParameter(OAUTH_SCOPE_PARAMETER_NAME, REDSHIFT_IDC_CONNECT_SCOPE)
            .addParameter(OAUTH_CSRF_STATE_PARAMETER_NAME, state)
            .addParameter(OAUTH_CODE_CHALLENGE_PARAMETER_NAME, codeChallenge)
            .addParameter(OAUTH_CHALLENGE_METHOD_PARAMETER_NAME, CHALLENGE_METHOD);
        URI authorizeRequestUrl;
        authorizeRequestUrl = builder.build();
        
        validateURL(authorizeRequestUrl.toString());           
        
		if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        	Desktop.getDesktop().browse(authorizeRequestUrl);
		} else {
			m_log.log(LogLevel.ERROR, "Unable to open the browser. Desktop environment is not supported");
		}
        
      	if(RedshiftLogger.isEnable())
	        m_log.log(LogLevel.DEBUG,
	            String.format("Authorization code request URI: \n%s", authorizeRequestUrl.toString()));
    }

	private String createIdcHost(String idc_region) {
		return OIDC_SUBDOMAIN + "." + m_idc_region + "." + AMAZON_COM_DOMAIN;
	}

	private byte[] sha256(byte[] input) {
        try {
            MessageDigest digest = MessageDigest.getInstance(SHA256_METHOD);
            return digest.digest(input);
        } catch (NoSuchAlgorithmException e) {
            if (RedshiftLogger.isEnable())
				m_log.log(LogLevel.ERROR, e, "Thread interrupted during sleep");
            return null;
        }
    }

	private boolean isCachedRegisterClientResponseValid(RegisterClientResponse cachedRegisterClientResponse) {
		if (cachedRegisterClientResponse == null || cachedRegisterClientResponse.clientSecretExpiresAt() == null) {
			return false;
		}
		return System.currentTimeMillis() < cachedRegisterClientResponse.clientSecretExpiresAt() * 1000;
	}
}
