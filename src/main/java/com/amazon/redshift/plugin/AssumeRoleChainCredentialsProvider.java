package com.amazon.redshift.plugin;

import com.amazon.redshift.IPlugin;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import static com.amazon.redshift.logger.LogLevel.INFO;

/**
 * Plugin to assume a role or a chain of roles first before using IAM authentication
 */
public class AssumeRoleChainCredentialsProvider implements IPlugin {

    private static final String CACHE_KEY_PREFIX = "AssumeRole_";

    /**
     * default session name for role assumption if not provided
     */
    private static final String DEFAULT_SESSION_NAME = "assumeRoleIamAuthJDBC";

    /**
     * Connection property that sets the role chain. The property can either contain a single role ARN
     * or a comma-separated list of role ARNs. The first role in the list is assumed first.
     */
    private String roleArn = null;

    /**
     *
     */
    private String sessionName = null;
    private RedshiftLogger log = null;
    private AWSCredentialsProvider credentialsProvider = null;


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
        return CACHE_KEY_PREFIX + (roleArn != null ? roleArn : "") + (sessionName != null ? sessionName : "");
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
            if (roleArn != null) {
                log(INFO, "Found roleArn: %s and sessionName: %s", roleArn, getNonEmptySessionName());

                String[] chainedRoleArns = roleArn.split(",");
                credentialsProvider = createChainedAssumeRoleSessionCredentialsProvider(chainedRoleArns);
            } else {
                log(INFO, "No roleArn found, using " + DefaultAWSCredentialsProviderChain.class.getSimpleName());
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
            }
        }
        return credentialsProvider;
    }

    protected AWSCredentialsProvider createChainedAssumeRoleSessionCredentialsProvider(String[] chainedRoleArns) {
        AWSCredentialsProvider result = null;

        for (String roleArn : chainedRoleArns) {
            log(INFO, "Assuming role: %s", roleArn);

            AWSSecurityTokenServiceClientBuilder stsClientBuilder = AWSSecurityTokenServiceClientBuilder.standard();
            if (result != null) {
                stsClientBuilder.withCredentials(result);
            }

            result = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, getNonEmptySessionName())
                    .withStsClient(stsClientBuilder.build())
                    .build();
        }

        return result;
    }

    protected String getNonEmptySessionName() {
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
