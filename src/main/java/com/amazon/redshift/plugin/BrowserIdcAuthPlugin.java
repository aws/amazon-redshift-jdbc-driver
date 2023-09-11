/**
 * Copyright 2010-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.services.ssooidc.AWSSSOOIDC;
import com.amazonaws.services.ssooidc.AWSSSOOIDCClientBuilder;
import com.amazonaws.services.ssooidc.model.*;
import com.amazonaws.util.StringUtils;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

/**
 * Class to get IdC Token from AWS Identity Center (IdC)
 */
public class BrowserIdcAuthPlugin extends CommonCredentialsProvider {

    /**
     * Key for setting AWS access portal start URL.
     */
    private static final String KEY_START_URL = "start_url";

    /**
     * Key for setting IdC client display name
     */
    private static final String KEY_IDC_CLIENT_DISPLAY_NAME = "idc_client_display_name";

    /**
     * Key for setting IdC region
     */
    private static final String KEY_IDC_REGION = "idc_region";

    /**
     * Key for setting IdC browser auth timeout value
     */
    private static final String KEY_IDC_RESPONSE_TIMEOUT = "idc_response_timeout";

    private static final String M_CLIENT_TYPE = "public";
    private static final String M_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code";
    private static final String M_SCOPE = "redshift:connect";

    private static final Map<String, RegisterClientResult> m_register_client_cache = new HashMap<String, RegisterClientResult>();

    /**
     * The default time in seconds for which the client must wait between attempts when polling for a session
     * It is used if auth server doesn't provide any value for {@code interval} in start device authorization response
     */
    public final int REQUEST_CREATE_TOKEN_DEFAULT_INTERVAL = 1;
    public final int DEFAULT_IDC_TOKEN_EXPIRY_IN_SEC = 900;

    protected AWSSSOOIDC m_sdk_client;

    private String m_idcRegion;
    private String m_startUrl;
    private String m_idcClientDisplayName = RedshiftProperty.IDC_CLIENT_DISPLAY_NAME.getDefaultValue();
    private int m_idcResponseTimeout = 120;

    public BrowserIdcAuthPlugin() {
    }

    public BrowserIdcAuthPlugin(AWSSSOOIDC client) {
        m_sdk_client = client;
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
            case KEY_START_URL:
                m_startUrl = value;
                if (RedshiftLogger.isEnable())
                    m_log.logDebug("Setting start_url: {0}", m_startUrl);
                break;

            case KEY_IDC_REGION:
                m_idcRegion = value;
                if (RedshiftLogger.isEnable())
                    m_log.logDebug("Setting idc_region: {0}", m_idcRegion);
                break;

            case KEY_IDC_CLIENT_DISPLAY_NAME:
                if (!StringUtils.isNullOrEmpty(value))
                    m_idcClientDisplayName = value;
                if (RedshiftLogger.isEnable())
                    m_log.logDebug("Setting idc_client_display_name: {0}", m_idcClientDisplayName);
                break;

            case KEY_IDC_RESPONSE_TIMEOUT:
                if (!StringUtils.isNullOrEmpty(value)) {
                    int timeout = Integer.parseInt(value);
                    if (timeout > 10) { // minimum allowed timeout value is 10 secs
                        m_idcResponseTimeout = timeout;
                        if (RedshiftLogger.isEnable())
                            m_log.logDebug("Setting idc_response_timeout: {0}", m_idcResponseTimeout);
                    } else { // else use default timeout value itself
                        if (RedshiftLogger.isEnable())
                            m_log.logDebug("Setting idc_response_timeout={0}; provided value={1}", m_idcResponseTimeout, timeout);
                    }
                }
                break;

            default:
                super.addParameter(key, value);
        }
    }

    /**
     * @return The cache key against which the idc token holder is stored, specific to this plugin
     */
    @Override
    public String getPluginSpecificCacheKey() {
        return ((m_startUrl != null) ? m_startUrl : "");
    }

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
     * Plugin implementation method to grab the IdC token from AWS IAM Identity Center.
     *
     * @return {@link NativeTokenHolder} A wrapper containing IdC token and its expiration time information
     * @throws IOException indicating the error
     */
    protected NativeTokenHolder getIdcToken() throws IOException {
        try {
            checkRequiredParameters();
            m_sdk_client = AWSSSOOIDCClientBuilder.standard().withRegion(m_idcRegion).build();

            RegisterClientResult registerClientResult = getRegisterClientResult(m_idcClientDisplayName, M_CLIENT_TYPE);

            StartDeviceAuthorizationResult startDeviceAuthorizationResult = getStartDeviceAuthorizationResult(
                    registerClientResult.getClientId(), registerClientResult.getClientSecret(), m_startUrl);

            openBrowser(startDeviceAuthorizationResult.getVerificationUriComplete());
            CreateTokenResult createTokenResult = fetchTokenResult(registerClientResult, startDeviceAuthorizationResult, M_GRANT_TYPE, M_SCOPE);
            return processCreateTokenResult(createTokenResult);
        } catch (InternalPluginException ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "InternalPluginException in getIdcToken");
            // Wrap any exception to be compatible with CommonCredentialsProvider API
            throw new IOException(ex.getMessage(), ex);
        }
    }

    private void checkRequiredParameters() throws InternalPluginException {
        if (isNullOrEmpty(m_startUrl)) {
            m_log.logDebug("IdC authentication failed: start_url needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The start URL must be included in the connection parameters.");
        }
        if (isNullOrEmpty(m_idcRegion)) {
            m_log.logDebug("IdC authentication failed: idc_region needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The IdC region must be included in the connection parameters.");
        }
    }

    /**
     * Registers a client with IAM Identity Center. This allows clients to initiate device authorization.
     * The output is persisted for reuse through many authentication requests.
     *
     * @param clientName The friendly name of the client
     * @param clientType The type of client. The service supports only {@code public} as a client type
     * @return {@link RegisterClientResult} Client registration result containing {@code clientId} and {@code clientSecret} required for device authorization
     * @throws IOException if an error occurs during the involved API call
     */
    protected RegisterClientResult getRegisterClientResult(String clientName, String clientType) throws IOException {
        String registerClientCacheKey = clientName + ":" + m_idcRegion;
        RegisterClientResult cachedRegisterClientResult = m_register_client_cache.get(registerClientCacheKey);
        if (isCachedRegisterClientResultValid(cachedRegisterClientResult)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Using cached register client result");
            return cachedRegisterClientResult;
        }

        RegisterClientRequest registerClientRequest = new RegisterClientRequest();
        registerClientRequest.withClientName(clientName);
        registerClientRequest.withClientType(clientType);
        registerClientRequest.withScopes(M_SCOPE);

        RegisterClientResult registerClientResult = null;
        try {
            registerClientResult = m_sdk_client.registerClient(registerClientRequest);
            if (RedshiftLogger.isEnable())
                m_log.logDebug("registerClient response code: {0}", registerClientResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (InternalServerException ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Error: Unexpected server error while registering client;");
            throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
        } catch (Exception ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Error: Unexpected register client error;");
            throw new IOException("IdC authentication failed : There was an error during authentication.", ex);
        }
        m_register_client_cache.put(registerClientCacheKey, registerClientResult);
        return registerClientResult;
    }

    private boolean isCachedRegisterClientResultValid(RegisterClientResult cachedRegisterClientResult) {
        if (cachedRegisterClientResult == null || cachedRegisterClientResult.getClientSecretExpiresAt() == null) {
            return false;
        }
        return System.currentTimeMillis() < cachedRegisterClientResult.getClientSecretExpiresAt() * 1000;
    }

    /**
     * Initiates device authorization by requesting a pair of verification codes from the IAM Identity Center
     *
     * @param clientId     The unique identifier string for the client that is registered with IAM Identity Center.
     * @param clientSecret A secret string that is generated for the client.
     * @param startUrl     The URL for the AWS access portal
     * @return {@link StartDeviceAuthorizationResult} Device Authorization result containing {@code deviceCode} for creating token
     * @throws IOException if an error occurs during the involved API call
     */
    protected StartDeviceAuthorizationResult getStartDeviceAuthorizationResult(String clientId, String clientSecret, String startUrl) throws IOException {
        StartDeviceAuthorizationRequest startDeviceAuthorizationRequest = new StartDeviceAuthorizationRequest();
        startDeviceAuthorizationRequest.withClientId(clientId);
        startDeviceAuthorizationRequest.withClientSecret(clientSecret);
        startDeviceAuthorizationRequest.withStartUrl(startUrl);

        StartDeviceAuthorizationResult startDeviceAuthorizationResult = null;
        try {
            startDeviceAuthorizationResult = m_sdk_client.startDeviceAuthorization(startDeviceAuthorizationRequest);
            if (RedshiftLogger.isEnable())
                m_log.logDebug("startDeviceAuthorization response code: {0}", startDeviceAuthorizationResult.getSdkHttpMetadata().getHttpStatusCode());
        } catch (SlowDownException ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Error: Too frequent requests made by client;");
            throw new IOException("IdC authentication failed : Requests to the IdC service are too frequent.", ex);
        } catch (InternalServerException ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Error: Server error in start device authorization;");
            throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
        } catch (Exception ex) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, ex, "Error: Unexpected error in start device authorization;");
            throw new IOException("IdC authentication failed : There was an error during authentication.", ex);
        }

        return startDeviceAuthorizationResult;
    }

    protected void openBrowser(String verificationUri) throws IOException {
        validateURL(verificationUri);

        Desktop.getDesktop().browse(URI.create(verificationUri));

        if (RedshiftLogger.isEnable())
            m_log.log(LogLevel.DEBUG,
                    String.format("Authorization code request URI: \n%s", verificationUri));
    }

    /**
     * Creates and returns an access token for the authorized client.
     * The access token issued will be used to fetch short-term credentials for the assigned roles in the AWS account.
     *
     * @param clientId     The unique identifier string for each client
     * @param clientSecret A secret string generated for the client
     * @param deviceCode   Used only when calling this API for the device code grant type. This short-term code is used to identify this authentication attempt
     * @param grantType    Supports grant types for the device code request
     * @param scope        The list of scopes that is defined by the client. Upon authorization, this list is used to restrict permissions when granting an access token
     * @return {@link CreateTokenResult} Create token result containing IdC token
     */
    protected CreateTokenResult getCreateTokenResult(String clientId, String clientSecret, String deviceCode, String grantType, String... scope) {
        CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.withClientId(clientId);
        createTokenRequest.withClientSecret(clientSecret);
        createTokenRequest.withDeviceCode(deviceCode);
        createTokenRequest.withGrantType(grantType);
        createTokenRequest.withScope(scope);

        return m_sdk_client.createToken(createTokenRequest);
    }

    protected CreateTokenResult fetchTokenResult(RegisterClientResult registerClientResult, StartDeviceAuthorizationResult startDeviceAuthorizationResult, String grantType, String scope) throws IOException {
        long pollingEndTime = System.currentTimeMillis() + m_idcResponseTimeout * 1000L;

        int pollingIntervalInSec = REQUEST_CREATE_TOKEN_DEFAULT_INTERVAL;
        if (startDeviceAuthorizationResult.getInterval() != null && startDeviceAuthorizationResult.getInterval() > 0) {
            pollingIntervalInSec = startDeviceAuthorizationResult.getInterval(); // min wait time between attempts
        }

        // poll for create token with pollingIntervalInSec wait time between each attempt until pollingEndTime
        while (System.currentTimeMillis() < pollingEndTime) {
            try {
                CreateTokenResult createTokenResult = getCreateTokenResult(registerClientResult.getClientId(), registerClientResult.getClientSecret(), startDeviceAuthorizationResult.getDeviceCode(), grantType, scope);
                if (RedshiftLogger.isEnable())
                    m_log.logDebug("createToken response code: {0}", createTokenResult.getSdkHttpMetadata().getHttpStatusCode());
                if (createTokenResult != null && createTokenResult.getAccessToken() != null) {
                    return createTokenResult;
                } else {
                    // auth server sent a non exception response without valid token, so throw error
                    if (RedshiftLogger.isEnable())
                        m_log.logError("Failed to fetch an IdC access token");
                    throw new IOException("IdC authentication failed : The credential token couldn't be created.");
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
                throw new IOException("IdC authentication failed : You don't have sufficient permission to perform the action.", ex);
            } catch (InternalServerException ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Error: Server error in creating token;");
                throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
            } catch (Exception ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Error: Unexpected error in create token;");
                throw new IOException("IdC authentication failed : There was an error during authentication.", ex);
            }

            try {
                Thread.sleep(pollingIntervalInSec * 1000L);
            } catch (InterruptedException ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Thread interrupted during sleep");
            }
        }

        if (RedshiftLogger.isEnable())
            m_log.logError("Error: Request timed out while waiting for user authentication in the browser");
        throw new IOException("IdC authentication failed : The request timed out. Authentication wasn't completed.");
    }

    protected NativeTokenHolder processCreateTokenResult(CreateTokenResult createTokenResult) throws IOException {
        String idcToken = createTokenResult.getAccessToken();
        int expiresInSecs;
        if (createTokenResult.getExpiresIn() != null && createTokenResult.getExpiresIn() > 0) {
            expiresInSecs = createTokenResult.getExpiresIn();
        } else {
            expiresInSecs = DEFAULT_IDC_TOKEN_EXPIRY_IN_SEC;
        }

        Date expiration = new Date(System.currentTimeMillis() + expiresInSecs * 1000L);
        return NativeTokenHolder.newInstance(idcToken, expiration);
    }

}
