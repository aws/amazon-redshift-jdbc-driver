package com.amazon.redshift.plugin;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import com.amazon.redshift.Driver;
import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.util.RedshiftProperties;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.logger.RedshiftLogger;
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.RedshiftClientBuilder;
import software.amazon.awssdk.services.redshiftserverless.RedshiftServerlessClient;
import software.amazon.awssdk.services.redshiftserverless.RedshiftServerlessClientBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;

/**
 * A basic credential provider class.
 * This plugin class allows clients to directly provide any auth token that is handled by Redshift.
 * It supports two authentication flows:
 * 1. Direct token flow: token_type and token parameters
 * 2. Identity-enhanced credentials flow: AccessKeyID, SecretAccessKey, and SessionToken parameters
 */
public class IdpTokenAuthPlugin extends CommonCredentialsProvider {

    private static final String KEY_TOKEN = "token";
    private static final String KEY_TOKEN_TYPE = "token_type";
    private static final int DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC = 900;
    private static final String KEY_ACCESS_KEY_ID = "AccessKeyID";
    private static final String KEY_SECRET_ACCESS_KEY = "SecretAccessKey";
    private static final String KEY_SESSION_TOKEN = "SessionToken";
    private static final String KEY_HOST = "Host";
    private static final String KEY_ENDPOINT_URL = "EndpointUrl";
    private static final String KEY_REGION = "Region";

    private String token;
    private String token_type;
    private String accessKeyId;
    private String secretAccessKey;
    private String sessionToken;
    private String host;
    private String endpointUrl;
    private String region;

    public IdpTokenAuthPlugin() {
    }

    /**
     * Helper class to hold cluster information extracted from hostname
     */
    private static class ClusterInfo {
        final String identifier;
        final String region;
        final boolean isServerless;
        final String accountId;

        ClusterInfo(String identifier, String region, boolean isServerless, String accountId) {
            this.identifier = identifier;
            this.region = region;
            this.isServerless = isServerless;
            this.accountId = accountId;
        }
    }

    /**
     * This overridden method needs to return the auth token provided by the client
     *
     * @return {@link NativeTokenHolder} A wrapper containing auth token and its expiration time information
     * @throws IOException indicating that some required parameter is missing.
     */
    @Override
    protected NativeTokenHolder getAuthToken() throws IOException {
        checkRequiredParameters();

        if (isUsingIdentityEnhancedCredentials()) {
            // Identity-enhanced credentials flow: make GetIdentityCenterAuthToken call to get subject token
            return getSubjectToken();
	    } else {
            // Direct token flow: use provided token directly
            Date expiration = new Date(System.currentTimeMillis() + DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC * 1000L);
            return NativeTokenHolder.newInstance(token, expiration);
	    }
    }

    /**
     * This function will check to ensure that we are using one of the valid sets of parameters for authentication with
     * IdPTokenAuthPlugin. There are two valid parameter combinations.
     * 1. token and token_type
     * 2. IdC enhanced credentials containing access key, secret access key, and session token
     */ 
    private void checkRequiredParameters() throws IOException {
        boolean hasTokenParams = !Utils.isNullOrEmpty(token) && !Utils.isNullOrEmpty(token_type);
        boolean hasIamParams = isUsingIdentityEnhancedCredentials();

        if (!hasTokenParams && !hasIamParams) {
            throw new IOException("IdC authentication failed: Either (token and token_type) or " +
                    "(AccessKeyID, SecretAccessKey, and SessionToken) must be provided in the connection parameters.");
        }

        // Do not support both types of parameters at once
        if (hasTokenParams && hasIamParams) {
            throw new IOException("IdC authentication failed: Cannot provide both token parameters " +
                    "(token, token_type) and IAM credential parameters (AccessKeyID, SecretAccessKey, SessionToken) " +
                    "at the same time. Please use only one authentication method.");
    }

        if (hasIamParams) {
            // For identity-enhanced credentials, we need the host URL to extract cluster and region
            if (Utils.isNullOrEmpty(host)) {
                throw new IOException("IdC authentication failed: Host URL must be provided " +
                    "to extract cluster identifier and region for identity-enhanced credentials.");
            }
        }
    }
    
    /**
     * Makes a call to GetIdentityCenterAuthToken API to obtain a subject token containing the IdC identity information.
     * This initializes provisioned Redshift client and is used only for provisioned clusters.
     */
    private String getProvisionedAuthToken(AwsCredentialsProvider credentialsProvider, String region, String clusterIdentifier) throws IOException {
        try {
            // Initialize standard Redshift client for provisioned clusters
            RedshiftClientBuilder builder = RedshiftClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region));

            // Apply endpoint override if provided
            if (!Utils.isNullOrEmpty(endpointUrl)) {
                builder.endpointOverride(URI.create(endpointUrl));
                if (RedshiftLogger.isEnable()) {
                    m_log.logDebug("Using custom endpoint URL for provisioned cluster: {0}", endpointUrl);
                }
            }

            try (RedshiftClient redshiftClient = builder.build()) {
                // Create request for GetIdentityCenterAuthToken API
                software.amazon.awssdk.services.redshift.model.GetIdentityCenterAuthTokenRequest request =
                    software.amazon.awssdk.services.redshift.model.GetIdentityCenterAuthTokenRequest.builder()
                        .clusterIds(Arrays.asList(clusterIdentifier))
                        .build();
                
                // Make the API call
                software.amazon.awssdk.services.redshift.model.GetIdentityCenterAuthTokenResponse response =
                    redshiftClient.getIdentityCenterAuthToken(request);
                
                if (response == null || Utils.isNullOrEmpty(response.token())) {
                    throw new IOException("GetIdentityCenterAuthToken returned empty response for provisioned cluster");
                }
                return response.token();
            }
        } catch (Exception e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError("Failed to get provisioned auth token: {0}", e.getMessage());
            }
            throw new IOException("Failed to obtain subject token from GetIdentityCenterAuthToken API for provisioned cluster", e);
        }
    }
	  
    /**
     * Makes a call to GetIdentityCenterAuthToken API to obtain a subject token containing the IdC identity information.
     * This initializes serverless Redshift client and is used only for serverless clusters.
     */
    private String getServerlessAuthToken(AwsCredentialsProvider credentialsProvider, String region, String workgroupName) throws IOException {
        try {
            // Initialize standard Redshift client (serverless APIs are part of the same client)
            RedshiftServerlessClientBuilder builder = RedshiftServerlessClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region));

            // Apply endpoint override if provided
            if (!Utils.isNullOrEmpty(endpointUrl)) {
                builder.endpointOverride(URI.create(endpointUrl));
                if (RedshiftLogger.isEnable()) {
                    m_log.logDebug("Using custom endpoint URL for serverless workgroup: {0}", endpointUrl);
                }
            }

            try (RedshiftServerlessClient redshiftClient = builder.build()){

                // Create request for GetIdentityCenterAuthToken API (serverless)
                software.amazon.awssdk.services.redshiftserverless.model.GetIdentityCenterAuthTokenRequest request =
                    software.amazon.awssdk.services.redshiftserverless.model.GetIdentityCenterAuthTokenRequest.builder().workgroupNames(Arrays.asList(workgroupName)).build();
                
                // Make API call
                software.amazon.awssdk.services.redshiftserverless.model.GetIdentityCenterAuthTokenResponse response =
                    redshiftClient.getIdentityCenterAuthToken(request);

                if (response == null || Utils.isNullOrEmpty(response.token())) {
                    throw new IOException("GetIdentityCenterAuthToken returned empty response for serverless workgroup");
                }
                return response.token();

            }
        } catch (Exception e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError("Failed to get serverless auth token: {0}", e.getMessage());
            }
            throw new IOException("Failed to obtain subject token from GetIdentityCenterAuthToken API for serverless workgroup", e);
        }
    }

    /**
     * Resolves cluster information by leveraging the existing Driver.parseHostName() method.
     * The host is automatically extracted from the JDBC URL hostname by NativeAuthPluginHelper.
     * This ensures consistent behavior with the main IAM authentication flow.
     * This API currently does not support CNAME.
     * @return ClusterInfo containing identifier, region, serverless flag, and account ID
     * @throws IOException if cluster information cannot be determined
     */
    private ClusterInfo resolveClusterInfo() throws IOException {
        if (Utils.isNullOrEmpty(host)) {
            throw new IOException("IdC authentication failed: Host URL is required to extract cluster identifier");
        }

        // Leverage existing Driver parsing logic
        RedshiftProperties tempProps = new RedshiftProperties();
        tempProps = Driver.parseHostName(tempProps, host);

        // Extract the parsed values
        String clusterId = tempProps.getProperty(RedshiftProperty.CLUSTER_IDENTIFIER.getName());
        String parsedRegion = tempProps.getProperty(RedshiftProperty.AWS_REGION.getName());
        String workGroup = tempProps.getProperty(RedshiftProperty.SERVERLESS_WORK_GROUP.getName());
        String acctId = tempProps.getProperty(RedshiftProperty.SERVERLESS_ACCT_ID.getName());

        // Use explicit region if provided, otherwise use parsed region from hostname
        String resolvedRegion = region;
        if (Utils.isNullOrEmpty(resolvedRegion)) {
            resolvedRegion = parsedRegion;
            m_log.logDebug("Identity enhanced credentials: using parsed region {0}",  parsedRegion);
        }
        else{
            m_log.logDebug("Identity enhanced credentials: using explicit region {0}",  region);
        }


        // Normalize region to lowercase (similar to IAM approach)
        if (!Utils.isNullOrEmpty(resolvedRegion)) {
            resolvedRegion = resolvedRegion.trim().toLowerCase();
        }

        if (RedshiftLogger.isEnable()) {
            m_log.logDebug("Resolved cluster info - clusterId: {0}, region: {1}, workGroup: {2}, acctId: {3}", 
                clusterId, resolvedRegion, workGroup, acctId);
        }

        // Determine if serverless based on workgroup presence
        boolean isServerless = !Utils.isNullOrEmpty(workGroup);

        String identifier = isServerless ? workGroup : clusterId;

        if (Utils.isNullOrEmpty(resolvedRegion)) {
            throw new IOException("IdC authentication failed: Unable to determine AWS region from hostname or connection parameters. Please provide an explicit region parameter.");
        }

        if (Utils.isNullOrEmpty(identifier)) {
            throw new IOException("IdC authentication failed: Unable to determine cluster identifier from hostname. Please verify the connection URL format.");
        }

        return new ClusterInfo(identifier, resolvedRegion, isServerless, acctId);
    }

    /**
     * Makes a call to GetIdentityCenterAuthToken API to obtain a subject token containing the IdC identity information.
     * Uses different APIs and clients based on whether the target is a provisioned cluster or serverless workgroup.
     * 
     * @return {@link NativeTokenHolder} containing the subject token and expiration
     * @throws IOException if the GetIdentityCenterAuthToken call fails or required parameters are missing
     */
    private NativeTokenHolder getSubjectToken() throws IOException {
        try {
            ClusterInfo clusterInfo = resolveClusterInfo();

            if (RedshiftLogger.isEnable()) {
                m_log.logDebug("Making GetIdentityCenterAuthToken call with {0}: {1}, region: {2}", 
                    clusterInfo.isServerless ? "workgroup" : "cluster", 
                    clusterInfo.identifier, clusterInfo.region);
            }

            // Initialize AWS credentials with identity-enhanced credentials
            AwsSessionCredentials credentials = AwsSessionCredentials.create(
                accessKeyId, secretAccessKey, sessionToken);
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

            String subjectToken;
            if (clusterInfo.isServerless) {
                // Use serverless API for workgroups
                subjectToken = getServerlessAuthToken(credentialsProvider, clusterInfo.region, clusterInfo.identifier);
            } else {
                // Use provisioned API for clusters
                subjectToken = getProvisionedAuthToken(credentialsProvider, clusterInfo.region, clusterInfo.identifier);
            }

            if (Utils.isNullOrEmpty(subjectToken)) {
                throw new IOException("IdC authentication failed: GetIdentityCenterAuthToken returned empty subject token");
            }

            if (RedshiftLogger.isEnable()) {
                m_log.logDebug("Successfully obtained subject token from GetIdentityCenterAuthToken");
            }

            // Return the subject token with default expiration
            // The actual expiration will be handled by PADB after decryption
            Date expiration = new Date(System.currentTimeMillis() + DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC * 1000L);
            return NativeTokenHolder.newInstance(subjectToken, expiration);

        } catch (Exception e) {
            if (RedshiftLogger.isEnable()) {
                m_log.logError("Failed to obtain subject token from GetIdentityCenterAuthToken: {0}", e.getMessage());
            }
            throw new IOException("IdC authentication failed: Unable to obtain subject token from GetIdentityCenterAuthToken API", e);
        }
    }

    @Override
    public void addParameter(String key, String value) {
        super.addParameter(key, value);

        if (KEY_TOKEN.equalsIgnoreCase(key)) {
            token = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token of length={0}", token.length());
        } else if (KEY_TOKEN_TYPE.equalsIgnoreCase(key)) {
            token_type = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token_type: {0}", token_type);
        } else if (KEY_ACCESS_KEY_ID.equalsIgnoreCase(key)) {
	             accessKeyId = value;
	             if (RedshiftLogger.isEnable())
	                 m_log.logDebug("Setting AccessKeyID");
        } else if (KEY_SECRET_ACCESS_KEY.equalsIgnoreCase(key)) {
            secretAccessKey = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting SecretAccessKey");
        } else if (KEY_SESSION_TOKEN.equalsIgnoreCase(key)) {
            sessionToken = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting SessionToken");
        } else if (KEY_HOST.equalsIgnoreCase(key)) {
            host = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting Host: {0}", host);
        } else if (KEY_ENDPOINT_URL.equalsIgnoreCase(key)) {
            endpointUrl = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting EndpointUrl: {0}", endpointUrl);
        } else if (KEY_REGION.equalsIgnoreCase(key)) {
            region = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting Region: {0}", region);
        }
    }

    /**
     * Check if this plugin is using identity-enhanced credentials (IAM credentials with IdC context)
     * Note that this function is not actually checking to see if the credentials themselves are
     * identity enhanced, it is just checking to see if they have the necessary parameters of
     * access key, secret access key, and session token provided
     * @return true if using identity-enhanced credentials, false if using direct token
     */
    public boolean isUsingIdentityEnhancedCredentials() {
        return !Utils.isNullOrEmpty(accessKeyId) &&
            !Utils.isNullOrEmpty(secretAccessKey) &&
            !Utils.isNullOrEmpty(sessionToken);
    }

}