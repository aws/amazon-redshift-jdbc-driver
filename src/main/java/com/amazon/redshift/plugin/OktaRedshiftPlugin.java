package com.amazon.redshift.plugin;

import java.util.Date;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.httpserver.RequestHandler;
import com.amazon.redshift.plugin.httpserver.Server;
import com.amazon.redshift.plugin.utils.RandomStateUtil;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.GetClusterCredentialsRequest;
import com.amazonaws.services.redshift.model.GetClusterCredentialsResult;
import com.amazonaws.services.sso.AWSSSO;
import com.amazonaws.services.sso.model.GetRoleCredentialsRequest;
import com.amazonaws.services.sso.model.GetRoleCredentialsResult;
import com.amazonaws.services.sso.model.RoleCredentials;
import com.amazonaws.services.ssooidc.AWSSSOOIDC;
import com.amazonaws.services.sso.AWSSSOClientBuilder;
import com.amazonaws.services.ssooidc.AWSSSOOIDCClientBuilder;
import com.amazonaws.services.ssooidc.model.*;
import com.amazonaws.util.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import static com.amazon.redshift.plugin.utils.ResponseUtils.findParameter;

import java.awt.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.function.Function;

/**
 * OktaRedshiftPlugin - Handles Okta-based authentication for Amazon Redshift connections
 * This plugin implements OAuth 2.0 authorization code flow with PKCE for secure authentication
 * through AWS SSO OIDC, followed by role assumption to obtain AWS credentials for Redshift access.
 */
public class OktaRedshiftPlugin extends IdpCredentialsProvider implements IPlugin {

    // Variables for SSO authentication configuration
    private String ssoRoleName;          // AWS SSO role name (e.g., "OktaAdminLogin")
    private String redshiftRoleArn;      // ARN of the Redshift role to assume
    private String ssoRegion;            // AWS region for SSO operations
    private String ssoStartUrl;          // SSO start URL for authentication
    private String ssoAccountId;         // AWS account ID for SSO operations
    
    // Variables for Redshift cluster connection
    private String clusterId;            // Redshift cluster identifier
    private String dbName;               // Database name
    private String dbUser;               // Database user name

    // OAuth 2.0 and OIDC configuration constants
    private static final String redirectUriBase = "http://127.0.0.1";           // Base URL for OAuth redirect
    private final int listenPort = 7890;                                        // Local port for OAuth callback
    private final String idcClientDisplayName = RedshiftProperty.IDC_CLIENT_DISPLAY_NAME.getDefaultValue(); // Client display name
    private static final String idcClientType = "public";                       // OAuth client type (public for PKCE)
    private static final String idcClientScope = "sso:account:access";           // OAuth scope for SSO account access
    private static final String authCodeGrantType = "authorization_code";       // OAuth 2.0 grant type
    public final int codeVerifierByteLength = 60;                              // PKCE code verifier length in bytes
    public static final String oauthCsrfStateParameterName = "state";           // OAuth state parameter name
    private static final String authCodeParameterName = "code";                 // Authorization code parameter name
    private final int idcResponseTimeout = 120;                                // Timeout for user authentication (seconds)
    public final long milliSecondMultiplier = 1000L;                          // Millisecond conversion factor
    int defaultIdcTimeoutExpiryInSecs = 1200;                                  // Default token expiry time (seconds)

    // Runtime configuration and client instances
    protected String redirectUri;        // Complete redirect URI for OAuth flow
    AWSSSOOIDC ssoOidcClient;           // AWS SSO OIDC client for token operations

    // Cache for storing client registration results to avoid repeated registration
    // Cache key format: <redirect_uri>:<sso_region>:<listen_port>
    private static final Map<String, RegisterClientResult> registerClientResultCache = new HashMap<String, RegisterClientResult>();


    /**
     * Main entry point for obtaining AWS credentials for Redshift connection.
     * Orchestrates the OAuth flow and role assumption process.
     *
     * @return AWSCredentials that can be used to call GetClusterCredentials
     */
    @Override
    public AWSCredentials getCredentials() {
        try {
            // Step 1: Get IdC access token through OAuth flow
            NativeTokenHolder idcToken = getIdcToken();

            // Step 2: Always require role assumption for this plugin
            if (StringUtils.isNullOrEmpty(redshiftRoleArn)) {
                throw new IOException("Redshift role ARN is required but not provided");
            }

            // Step 3: Use IdC token to assume Redshift role and get AWS credentials
            return getAwsCredentials(idcToken);
        } catch (Exception e) {
            if (RedshiftLogger.isEnable())
                m_log.log(LogLevel.ERROR, e, "Error getting AWS credentials");
            throw new RuntimeException("Failed to get AWS credentials", e);
        }
    }

    @Override
    public void refresh() {
        // Credentials will be refreshed automatically when getCredentials() is called
    }

    // IPlugin interface methods
    @Override
    public void setLogger(RedshiftLogger log) {
        m_log = log;
    }

    @Override
    public void setGroupFederation(boolean groupFederation) {
        // Not used by this plugin
    }

    @Override
    public String getIdpToken() {
        // Not used by regular credential providers
        return null;
    }

    @Override
    public String getCacheKey() {
        return getPluginSpecificCacheKey();
    }

    @Override
    public int getSubType() {
        return 0; // Default subtype
    }

    @Override
    public String getPluginSpecificCacheKey() {
        return String.format("OktaRedshift_%s_%s_%s_%s_%s",
                ssoStartUrl != null ? ssoStartUrl : "",
                ssoRegion != null ? ssoRegion : "",
                ssoAccountId != null ? ssoAccountId : "",
                ssoRoleName != null ? ssoRoleName : "",
                redshiftRoleArn != null ? redshiftRoleArn : "");
    }

    /**
     * Executes the complete OAuth 2.0 authorization code flow with PKCE to obtain an IdC access token.
     *
     * @return NativeTokenHolder containing the IdC access token
     * @throws IOException        if the OAuth flow fails
     * @throws URISyntaxException if URL construction fails
     */
    private NativeTokenHolder getIdcToken() throws IOException, URISyntaxException {
        // Validate all required parameters before starting OAuth flow
        checkRequiredParameters();

        // Initialize SSO OIDC client for the specified region
        ssoOidcClient = AWSSSOOIDCClientBuilder.standard().withRegion(ssoRegion).build();
        redirectUri = redirectUriBase + ":" + listenPort;

        // Step 1: Register OAuth client or retrieve from cache
        RegisterClientResult registerClientResult = getRegisterClientResult();

        // Step 2: Generate PKCE code verifier and challenge for security
        String codeVerifier = generateCodeVerifier();
        String codeChallenge = generateCodeChallenge(codeVerifier);

        // Step 3: Open browser and get authorization code from user
        String authCode = fetchAuthorizationCode(codeChallenge, registerClientResult);

        // Step 4: Exchange authorization code for access token
        CreateTokenResult createTokenResult = fetchTokenResult(registerClientResult, authCode, codeVerifier);

        // Step 5: Process token result and return wrapped token
        return processCreateTokenResult(createTokenResult);
    }


    private void checkRequiredParameters() throws InternalPluginException {
        if (StringUtils.isNullOrEmpty(ssoStartUrl)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("IdC authentication failed: issuer_url needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The issuer URL must be included in the connection parameters.");
        }
        if (StringUtils.isNullOrEmpty(ssoRegion)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("IdC authentication failed: idc_region needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The IdC region must be included in the connection parameters.");
        }
        if (StringUtils.isNullOrEmpty(redshiftRoleArn)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("IdC authentication failed: redshift_role_arn needs to be provided in connection params");
            throw new InternalPluginException("redshift_role_arn is required");
        }
        if (StringUtils.isNullOrEmpty(ssoAccountId)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("IdC authentication failed: ssoAccountID needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The SSO account ID must be included in the connection parameters.");
        }
        if (StringUtils.isNullOrEmpty(ssoRoleName)) {
            if (RedshiftLogger.isEnable())
                m_log.logDebug("IdC authentication failed: ssoRoleName needs to be provided in connection params");
            throw new InternalPluginException("IdC authentication failed: The SSO role name must be included in the connection parameters.");
        }
    }

    private RegisterClientResult getRegisterClientResult() throws IOException {
        String registerClientCacheKey = redirectUri + ":" + ssoRegion + ":" + listenPort;
        RegisterClientResult cachedRegisterClientResult = registerClientResultCache.get(registerClientCacheKey);

        if (isCachedRegisteredClientValid(cachedRegisterClientResult)) {
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("Using cached client result");
                m_log.logInfo("Cached client result expires in " + cachedRegisterClientResult.getClientSecretExpiresAt());
            }
            return cachedRegisterClientResult;
        }

        RegisterClientRequest registerClientRequest = new RegisterClientRequest();
        registerClientRequest.withClientName(idcClientDisplayName);
        registerClientRequest.withClientType(idcClientType);
        registerClientRequest.withScopes(idcClientScope);
        registerClientRequest.withIssuerUrl(ssoStartUrl);
        registerClientRequest.withRedirectUris(redirectUri);
        registerClientRequest.withGrantTypes(authCodeGrantType);

        RegisterClientResult registerClientResult = null;

        try {
            registerClientResult = ssoOidcClient.registerClient(registerClientRequest);
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("Register client response code {0}", registerClientResult.getSdkHttpMetadata().getHttpStatusCode());
            }
        } catch (InternalServerException e) {
            if (RedshiftLogger.isEnable()) {
                m_log.log(LogLevel.ERROR, e, "Idc authentication failed: Error during the request");
            }
            throw new IOException("Idc authentication failed");
        } catch (Exception e) {
            if (RedshiftLogger.isEnable()) {
                m_log.log(LogLevel.ERROR, e, "Error while registering client");
            }
            throw new IOException("IdC registration failed");
        }

        registerClientResultCache.put(registerClientCacheKey, registerClientResult);
        if (RedshiftLogger.isEnable()) {
            m_log.logInfo("Cached the register client result, expires at {0}", registerClientResult.getClientSecretExpiresAt());
        }

        return registerClientResult;
    }


    private CreateTokenResult fetchTokenResult(RegisterClientResult registerClientResult, String authCode, String codeVerifier) throws IOException {
        long pollingEndtime = System.currentTimeMillis() + idcResponseTimeout * milliSecondMultiplier;

        int pollingIntervalInSec = 1;

        while (System.currentTimeMillis() < pollingEndtime) {
            try {
                CreateTokenRequest createTokenRequest = new CreateTokenRequest();
                createTokenRequest.withClientId(registerClientResult.getClientId())
                        .withClientSecret(registerClientResult.getClientSecret())
                        .withCode(authCode)
                        .withGrantType(authCodeGrantType)
                        .withCodeVerifier(codeVerifier)
                        .withRedirectUri(redirectUri);

                CreateTokenResult createTokenResult = ssoOidcClient.createToken(createTokenRequest);

                if (RedshiftLogger.isEnable() && registerClientResult.getSdkHttpMetadata() != null)
                    m_log.logDebug("Token response received");

                if (createTokenResult != null && createTokenResult.getAccessToken() != null) {
                    return createTokenResult;
                } else {
                    if (RedshiftLogger.isEnable()) m_log.logError("Failed to get IdC Token");
                    throw new IOException("IdC authentication failed: Failed to get IdC Token");
                }
            } catch (AuthorizationPendingException ex) {
                if (RedshiftLogger.isEnable()) m_log.logDebug("Browser authorization pending from user");
            } catch (SlowDownException ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Error: Too frequent createToken requests made by client;");
                throw new IOException("IdC authentication failed : Requests to the IdC service are too frequent.", ex);
            } catch (AccessDeniedException ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Error: Access denied, please ensure app assignment is done for the user;");
                throw new IOException("IdC authentication failed : You don't have sufficient permission to perform the action. Please ensure app assignment is done for the user.", ex);
            } catch (InternalServerException ex) {
                if (RedshiftLogger.isEnable()) m_log.log(LogLevel.ERROR, ex, "Error: Server error in creating token;");
                throw new IOException("IdC authentication failed : An error occurred during the request.", ex);
            } catch (Exception ex) {
                if (RedshiftLogger.isEnable())
                    m_log.log(LogLevel.ERROR, ex, "Error: Unexpected error in create token;");
                throw new IOException("IdC createToken failed : There was an error during the request.", ex);
            }
        }

        try {
            Thread.sleep(pollingIntervalInSec * milliSecondMultiplier);
        } catch (InterruptedException e) {
            if (RedshiftLogger.isEnable()) m_log.log(LogLevel.ERROR, e, "Thread interrupted during sleep");
        }

        if (RedshiftLogger.isEnable())
            m_log.logError("Error: Request timed out while waiting for user authentication in the browser");
        throw new IOException("IdC authentication failed : The request timed out. Authentication wasn't completed.");
    }

    private NativeTokenHolder processCreateTokenResult(CreateTokenResult createTokenResult) {
        String idcToken = createTokenResult.getAccessToken();

        if (StringUtils.isNullOrEmpty((idcToken))) {
            throw new InternalPluginException("Returned token result is null or empty");
        }

        int expiresInSec = defaultIdcTimeoutExpiryInSecs;

        if (createTokenResult.getExpiresIn() != null && createTokenResult.getExpiresIn() > 0) {
            expiresInSec = createTokenResult.getExpiresIn();
        }
        Date expiresInDate = new Date(System.currentTimeMillis() + expiresInSec * milliSecondMultiplier);
        if (RedshiftLogger.isEnable()) m_log.logDebug("Token expires at {0}", expiresInDate);

        return NativeTokenHolder.newInstance(idcToken, expiresInDate);
    }


    private AWSCredentials getAwsCredentials(NativeTokenHolder idcToken) throws IOException {
            // Get SSO role credentials
            AWSSSO sso = AWSSSOClientBuilder.standard().withRegion(ssoRegion).build();
            GetRoleCredentialsRequest getRoleRequest = new GetRoleCredentialsRequest()
                    .withAccessToken(idcToken.getAccessToken())
                    .withAccountId(ssoAccountId)
                    .withRoleName(ssoRoleName);

            GetRoleCredentialsResult roleCredentialsResult = sso.getRoleCredentials(getRoleRequest);
            RoleCredentials roleCredentials = roleCredentialsResult.getRoleCredentials();

            // Create session credentials from SSO role
            BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                    roleCredentials.getAccessKeyId(),
                    roleCredentials.getSecretAccessKey(),
                    roleCredentials.getSessionToken());

            // Assume the preferred role using SSO credentials
            String roleArn = redshiftRoleArn;
            if (!redshiftRoleArn.startsWith("arn:aws:iam::")) {
                roleArn = "arn:aws:iam::" + ssoAccountId + ":role/" + redshiftRoleArn;
            }

            AWSSecurityTokenService awsSTS = AWSSecurityTokenServiceClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .withRegion(ssoRegion)
                    .build();

            AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
                    .withRoleArn(roleArn)
                    .withRoleSessionName("redshift-okta-" + java.util.UUID.randomUUID())
                    .withDurationSeconds(3600);

            AssumeRoleResult assumeRoleResult = awsSTS.assumeRole(assumeRoleRequest);
            Credentials stsCredential = assumeRoleResult.getCredentials();

            // Create credentials for the assumed role
            BasicSessionCredentials redshiftRoleCredentials = new BasicSessionCredentials(
                    stsCredential.getAccessKeyId(),
                    stsCredential.getSecretAccessKey(),
                    stsCredential.getSessionToken());

            // Call GetClusterCredentials using the assumed role credentials
            AmazonRedshift redshiftClient = AmazonRedshiftClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(redshiftRoleCredentials))
                    .withRegion("eu-west-1")
                    .build();

            // Extract database user from preferred_role
            String dbUserName = this.redshiftRoleArn;
            if (dbUserName != null && dbUserName.contains("/")) {
                dbUserName = dbUserName.substring(dbUserName.lastIndexOf("/") + 1).toLowerCase();
            }
            if (dbUserName == null || dbUserName.isEmpty()) {
                dbUserName = "redshift_user"; // fallback
            }

            GetClusterCredentialsRequest clusterCredentialsRequest = new GetClusterCredentialsRequest()
                    .withClusterIdentifier(clusterId)
                    .withDbName(dbName)
                    .withDbUser(dbUserName)
                    .withDurationSeconds(3600);

            if (RedshiftLogger.isEnable()) {
                m_log.log(LogLevel.DEBUG, "Calling GetClusterCredentials for cluster: {0}, db: {1}, user: {2}", 
                         clusterId, dbName, dbUserName);
            }

            GetClusterCredentialsResult clusterCredentialsResult = redshiftClient.getClusterCredentials(clusterCredentialsRequest);
            
            // Return database credentials
            return new DatabaseCredentials(
                    clusterCredentialsResult.getDbUser(),
                    clusterCredentialsResult.getDbPassword(),
                    clusterCredentialsResult.getExpiration()
            );
    }

    protected String generateCodeVerifier() {
        byte[] randomBytes = new byte[codeVerifierByteLength];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(randomBytes);

        return Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
    }

    private String generateCodeChallenge(String codeVerifier) {
        byte[] sha256Hash = sha256(codeVerifier.getBytes(StandardCharsets.US_ASCII));

        return Base64.getUrlEncoder().withoutPadding().encodeToString(sha256Hash);
    }

    private byte[] sha256(byte[] input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(input);
        } catch (NoSuchAlgorithmException e) {
            if (RedshiftLogger.isEnable()) m_log.log(LogLevel.ERROR, e, "Thread interrupted during sleep");
            return null;
        }
    }

    private String fetchAuthorizationCode(String codeChallenge, RegisterClientResult registerClientResult) throws URISyntaxException, IOException {
        final String state = RandomStateUtil.generateRandomState();
        RequestHandler requestHandler = new RequestHandler(new Function<List<NameValuePair>, Object>() {
            public Object apply(List<NameValuePair> nameValuePairs) {
                String incomingState = findParameter(oauthCsrfStateParameterName, nameValuePairs);

                if (!state.equals(incomingState)) {
                    String stateErrorMessage = "Incoming state" + incomingState + " does not match the outgoing state" + state;
                    if (RedshiftLogger.isEnable()) m_log.log(LogLevel.DEBUG, stateErrorMessage);
                    throw new InternalPluginException(stateErrorMessage);
                }
                String code = findParameter(authCodeParameterName, nameValuePairs);
                if (StringUtils.isNullOrEmpty(code)) {
                    String stateErrorMessage = "No Valid code found";
                    if (RedshiftLogger.isEnable()) m_log.log(LogLevel.DEBUG, stateErrorMessage);
                    throw new InternalPluginException(stateErrorMessage);
                }
                return code;
            }
        });

        Server server = new Server(listenPort, requestHandler, Duration.ofSeconds(idcResponseTimeout), m_log);
        try {
            server.listen();
            if (RedshiftLogger.isEnable()) m_log.log(LogLevel.DEBUG, "Listening for connection on port " + listenPort);

            openBrowser(state, codeChallenge, registerClientResult);
            server.waitForResult();
        } catch (URISyntaxException | IOException ex) {
            if (RedshiftLogger.isEnable()) m_log.logError(ex);

            server.stop();
            throw ex;
        }

        Object result = requestHandler.getResult();

        if (result instanceof InternalPluginException) {
            if (RedshiftLogger.isEnable()) {
                m_log.logDebug("Error while fetching authorization token");
            }
            throw (InternalPluginException) result;
        }
        if (result instanceof String) {
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("Fetched authorization token");
            }
            return (String) result;
        }
        throw new InternalPluginException("Error fetching authentication code from browser. Failed to login during timeout.");
    }

    private void openBrowser(String state, String codeChallenge, RegisterClientResult registerClientResult) throws URISyntaxException, IOException {
        String idcHost = "oidc" + "." + ssoRegion + "." + "amazonaws.com";

        URIBuilder builder = new URIBuilder().setScheme("https")
                .setHost(idcHost)
                .setPath("/authorize")
                .addParameter("response_type", authCodeParameterName)
                .addParameter("client_id", registerClientResult.getClientId())
                .addParameter("redirect_uri", redirectUri)
                .addParameter("scopes", idcClientScope)
                .addParameter(oauthCsrfStateParameterName, state)
                .addParameter("code_challenge", codeChallenge)
                .addParameter("code_challenge_method", "S256");

        // Add account ID to scope the token to the specific account
        if (!StringUtils.isNullOrEmpty(ssoAccountId)) {
            builder.addParameter("account_id", ssoAccountId);
        }

        URI authorizeRequestUrl;
        authorizeRequestUrl = builder.build();

        validateURL(authorizeRequestUrl.toString());

        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
            Desktop.getDesktop().browse(authorizeRequestUrl);
        } else {
            m_log.logError("Unable to open the browser. Desktop environment is not supported");
        }

        if (RedshiftLogger.isEnable())
            m_log.logDebug("Authorization code request URI: \n%s", authorizeRequestUrl.toString());

    }

    private boolean isCachedRegisteredClientValid(RegisterClientResult cachedRegisterClientResult) {
        if (cachedRegisterClientResult == null || cachedRegisterClientResult.getClientSecretExpiresAt() == null) {
            return false;
        }

        return System.currentTimeMillis() < cachedRegisterClientResult.getClientSecretExpiresAt() * 1000;
    }

    @Override
    public void addParameter(String key, String value) {
        if ("ssorolename".equalsIgnoreCase(key)) {
            this.ssoRoleName = value;
        } else if ("preferred_role".equalsIgnoreCase(key)) {
            this.redshiftRoleArn = value;
        } else if ("region".equalsIgnoreCase(key)) {
            this.ssoRegion = value;
        } else if ("ssostarturl".equalsIgnoreCase(key)) {
            this.ssoStartUrl = value;
        } else if ("ssoaccountid".equalsIgnoreCase(key)) {
            this.ssoAccountId = value;
        } else if ("clusterid".equalsIgnoreCase(key)) {
            this.clusterId = value;
        } else if ("dbname".equalsIgnoreCase(key)) {
            this.dbName = value;
        } else if ("dbuser".equalsIgnoreCase(key)) {
            this.dbUser = value;
        }
    }
    public static void main(String[] args) throws Exception {
        // String profileName = "aws-sso-LunarWay-Development-Data-OktaDataLogin";
        // String profileName = "aws-sso-LunarWay-Development-Data-OktaAdminLogin";
        // why is this not set in .aws/config
        // "aws-sso-LunarWay-Production-Data-OktaDataViewer";

        OktaRedshiftPlugin plugin = new OktaRedshiftPlugin();
        plugin.addParameter("ssoRoleName", "OktaDataViewer");
        plugin.addParameter("region", "eu-north-1");
        plugin.addParameter("ssoStartUrl", "https://d-c3672deb5f.awsapps.com/start");
        plugin.addParameter("preferred_role", "hubble-rbac/DataViewer");
        // arn:aws:iam::899945594626:role/hubble-rbac/DataViewer
        plugin.addParameter("ssoAccountID", "899945594626");

        AWSCredentials  creds = plugin.getCredentials();

        System.out.println(creds.getAWSAccessKeyId());
    }
    
    /**
     * Simple holder for database credentials (username/password)
     */
    public static class DatabaseCredentials implements AWSCredentials {
        private final String username;
        private final String password;
        private final Date expiration;
        
        public DatabaseCredentials(String username, String password, Date expiration) {
            this.username = username;
            this.password = password;
            this.expiration = expiration;
        }
        
        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public Date getExpiration() { return expiration; }
        
        // AWSCredentials interface - store username/password in these fields
        @Override public String getAWSAccessKeyId() { return username; }
        @Override public String getAWSSecretKey() { return password; }
    }

}

