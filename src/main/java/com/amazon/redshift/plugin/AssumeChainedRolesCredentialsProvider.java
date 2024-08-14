package com.amazon.redshift.plugin;

import com.amazon.redshift.IPlugin;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import static com.amazon.redshift.logger.LogLevel.INFO;

/**
 * Plugin to assume a role or a chain of roles to authenticate via IAM-based auth in Redshift.
 * Set the role_arn parameter to the ARN of the role to assume. If you want to assume multiple roles in a chain, separate
 * the ARNs with commas. The plugin will assume the roles in the order they are provided.
 * Optionally, set the session_name parameter to a custom session name. If not provided, the default session name will be used.
 */
public class AssumeChainedRolesCredentialsProvider implements IPlugin {

    private static final String CACHE_KEY = AssumeChainedRolesCredentialsProvider.class.getName();
    private static final String DEFAULT_SESSION_NAME = CACHE_KEY;
    private AWSCredentialsProvider credentialsProvider = null;

    private String roleArn = null;
    private String sessionName = null;
    private RedshiftLogger log = null;


    @Override
    public void addParameter(String key, String value) {
        if (key.equals("role_arn")) {
            roleArn = value;
        }

        if (key.equals("session_name")) {
            sessionName = value;
        }
    }

    @Override
    public void setLogger(RedshiftLogger log) {
        this.log = log;
    }

    @Override
    public String getPluginSpecificCacheKey() {
        return CACHE_KEY;
    }

    @Override
    public void setGroupFederation(boolean groupFederation) {
    }

    @Override
    public String getIdpToken() {
        return null;
    }

    @Override
    public String getCacheKey() {
        return getPluginSpecificCacheKey();
    }

    @Override
    public int getSubType() {
        return 0;
    }

    @Override
    public AWSCredentials getCredentials() {
        return getCredentialsProvider().getCredentials();
    }

    protected AWSCredentialsProvider getCredentialsProvider() {
        if (credentialsProvider == null) {
            log(INFO, "Creating new credentials provider");
            if (roleArn != null && !roleArn.isEmpty()) {
                log(INFO, "Found roleArn: %s and sessionName: %s", roleArn, getNonEmptySessionName());
                String[] rolesArns = roleArn.split(",");
                AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
                for (String roleArn : rolesArns) {
                    credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, getNonEmptySessionName())
                            .withStsClient(stsClient)
                            .build();
                    stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                            .withCredentials(credentialsProvider)
                            .build();
                }
            } else {
                log(INFO, "No roleArn found, using DefaultAWSCredentialsProviderChain");
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
            }
        }
        return credentialsProvider;
    }

    private String getNonEmptySessionName() {
        return sessionName != null ? sessionName : DEFAULT_SESSION_NAME;
    }

    protected void log(LogLevel logLevel, String msg, Object... msgArgs) {
        if (RedshiftLogger.isEnable()) {
            log.log(logLevel, msg, msgArgs);
        }
    }

    @Override
    public void refresh() {
        getCredentialsProvider().refresh();
    }
}
