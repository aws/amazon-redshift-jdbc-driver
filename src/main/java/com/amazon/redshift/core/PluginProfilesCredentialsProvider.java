package com.amazon.redshift.core;

import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.profiles.Profile;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;

public class PluginProfilesCredentialsProvider implements AwsCredentialsProvider {
    private static final String PROFILE_PREFIX = "profile ";
    private static final String SOURCE_PROFILE = "source_profile";
    private static final String PLUGIN_NAME = "plugin_name";
    private static final String ROLE_ARN = "role_arn";
    private static final String ROLE_SESSION_NAME = "role_session_name";
    private static final String ROLE_EXTERNAL_ID = "role_external_id";
    private static final String CREDENTIAL_PROCESS = "credential_process";
    private static final String REDSHIFT_JDBC_PREFIX = "redshift-jdbc-";

    private Map<String, CredentialsHolder> cache = new ConcurrentHashMap<>();
    private RedshiftJDBCSettings m_settings;
    private RedshiftLogger m_log;

    private final ProfileFile profileFile;

    public PluginProfilesCredentialsProvider(RedshiftJDBCSettings settings, RedshiftLogger log) {
        m_settings = settings;
        m_log = log;
        this.profileFile = ProfileFile.defaultProfileFile();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return getCredentials(m_settings.m_profile);
    }

    /**
     * Returns the AWS credentials for the specified profile.
     */
    public CredentialsHolder getCredentials(String profileName) {
        CredentialsHolder credentials = cache.get(profileName);
        if (credentials == null) {
            // in case if profile is in ~/.aws/config file, check for 'profile ' prefix
            credentials = cache.get(PROFILE_PREFIX + profileName);
        }
        if (credentials != null && !credentials.isExpired()) {
            if (RedshiftLogger.isEnable()) {
                Date now = new Date();
                m_log.logInfo(now + ": Using existing entry for PluginProfilesConfigFile.getCredentials cache with expiration " + credentials.getExpiration());
            }
            return credentials;
        }

        // Get profile using V2 ProfileFile
        if (RedshiftLogger.isEnable()) {
            Map<String, Profile> profiles = profileFile.profiles();
            m_log.logInfo("profiles:" + profiles.keySet());
        }

        Profile profile = profileFile.profile(profileName)
                .isPresent() ? profileFile.profile(profileName).get()
                : profileFile.profile(PROFILE_PREFIX + profileName)
                .orElseThrow(() -> SdkClientException.create("No AWS profile named '" + profileName + "'"));

        if (isRoleBasedProfile(profile)) {
            String sourceProfile = profile.property(SOURCE_PROFILE)
                    .orElseThrow(() -> SdkClientException.create("Missing source_profile in profile"));
            if (Utils.isNullOrEmpty(sourceProfile)) {
                throw SdkClientException.create("Unable to load credentials from role based profile [" + profileName + "]: Source profile name is not specified");
            }
            CredentialsHolder srcCred = getCredentials(sourceProfile);
            AwsCredentialsProvider provider = StaticCredentialsProvider.create(srcCred);
            credentials = assumeRole(profile, provider);
            credentials.setMetadata(srcCred.getMetadata());
            cache.put(profileName, credentials);
            if (RedshiftLogger.isEnable()) {
                Date now = new Date();
                m_log.logInfo(now + ": Adding new role based entry for PluginProfilesConfigFile.getCredentials cache with expiration " + credentials.getExpiration());
            }
            return credentials;
        }

        String dbUser = null;
        String autoCreate = null;
        String dbGroups = null;
        String forceLowercase = null;

        String pluginName = profile.property(PLUGIN_NAME)
                .orElse(null);
        if (!Utils.isNullOrEmpty(pluginName)) {
            try {
                Class<? extends AwsCredentialsProvider> clazz =
                        (Class.forName(pluginName).asSubclass(AwsCredentialsProvider.class));

                AwsCredentialsProvider p = clazz.newInstance();
                if (p instanceof IPlugin) {
                    IPlugin plugin = (IPlugin) p;
                    plugin.setLogger(m_log);
                    Map<String, String> properties = profile.properties();
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        String key = entry.getKey().toLowerCase(Locale.getDefault());
                        if (!PLUGIN_NAME.equals(key)) {
                            String value = entry.getValue();
                            plugin.addParameter(key, value);

                            // DbUser value in connection string
                            if (RedshiftProperty.DB_USER.getName().equalsIgnoreCase(key)) {
                                dbUser = value;
                            } else if (RedshiftProperty.DB_GROUPS.getName().equalsIgnoreCase(key)) {
                                dbGroups = value;
                            } else if (RedshiftProperty.FORCE_LOWERCASE.getName().equalsIgnoreCase(key)) {
                                forceLowercase = value;
                            } else if (RedshiftProperty.USER_AUTOCREATE.getName().equalsIgnoreCase(key)) {
                                autoCreate = value;
                            }
                        }
                    }

                    // Add parameters from URL to plugin, override parameters from profile
                    for (Map.Entry<String, String> entry : m_settings.m_pluginArgs.entrySet()) {
                        String key = entry.getKey().toLowerCase(Locale.getDefault());
                        if (!PLUGIN_NAME.equals(key)) {
                            plugin.addParameter(entry.getKey(), entry.getValue());
                        }
                    }
                }

                credentials = CredentialsHolder.newInstance(p.resolveCredentials());
            } catch (InstantiationException e) {
                throw SdkClientException.create("Invalid plugin: '" + pluginName + "'");
            } catch (IllegalAccessException e) {
                throw SdkClientException.create("Invalid plugin: '" + pluginName + "'");
            } catch (ClassNotFoundException e) {
                throw SdkClientException.create("Invalid plugin: '" + pluginName + "'");
            }
        } else if (isProcessBasedProfile(profile)) {
            ProcessCredentialsProvider provider = ProcessCredentialsProvider.builder()
                    .command(profile.property(CREDENTIAL_PROCESS)
                            .orElseThrow(() -> SdkClientException.create("Missing credential_process in profile")))
                    .build();

            credentials = CredentialsHolder.newInstance(provider.resolveCredentials());
        } else {
            ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                    .profileName(profile.name())
                    .build();
            AwsCredentials c = credentialsProvider.resolveCredentials();
            credentials = CredentialsHolder.newInstance(c);
        }

        // override DbUser, AutoCreate , DbGroups, and ForceLowercase if null and defined in profile
        CredentialsHolder.IamMetadata metadata = credentials.getMetadata();
        if (null == metadata) {
            metadata = new CredentialsHolder.IamMetadata();
        }

        if (null != dbUser) {
            metadata.setProfileDbUser(dbUser);
        }

        if (null != autoCreate) {
            metadata.setAutoCreate(Boolean.valueOf(autoCreate));
        }
        if (null != dbGroups) {
            metadata.setDbGroups(dbGroups);
        }
        if (null != forceLowercase) {
            metadata.setForceLowercase(Boolean.valueOf(forceLowercase));
        }
        credentials.setMetadata(metadata);

        cache.put(profileName, credentials);
        if (RedshiftLogger.isEnable()) {
            Date now = new Date();
            m_log.logInfo(now + ": Using entry for PluginProfilesConfigFile.getCredentials cache with expiration " + credentials.getExpiration());
        }
        return credentials;
    }

    private CredentialsHolder assumeRole(Profile profile, AwsCredentialsProvider provider) {
        StsClientBuilder builder = StsClient.builder();

        StsClient stsSvc;
        try {
            stsSvc = RequestUtils.buildSts(m_settings.m_stsEndpoint, m_settings.m_awsRegion, builder, provider, m_log);
        } catch (Exception e) {
            throw SdkClientException.create("Profile Plugin error: " + e.getMessage(), e);
        }

        String roleArn = profile.property(ROLE_ARN)
                .orElseThrow(() -> SdkClientException.create("Missing role_arn in profile"));

        String roleSessionName = profile.property(ROLE_SESSION_NAME)
                .orElse(REDSHIFT_JDBC_PREFIX + System.currentTimeMillis());

        AssumeRoleRequest.Builder requestBuilder = AssumeRoleRequest.builder()
                .roleArn(roleArn)
                .roleSessionName(roleSessionName);

        profile.property(ROLE_EXTERNAL_ID).ifPresent(requestBuilder::externalId);

        try {
            AssumeRoleResponse result = stsSvc.assumeRole(requestBuilder.build());
            Credentials stsCredentials = result.credentials();

            AwsCredentials c = AwsSessionCredentials.create(
                    stsCredentials.accessKeyId(),
                    stsCredentials.secretAccessKey(),
                    stsCredentials.sessionToken());

            return CredentialsHolder.newInstance(c, stsCredentials.expiration());
        } finally {
            stsSvc.close();
        }
    }

    private boolean isProcessBasedProfile(Profile profile) {
        return profile.property(CREDENTIAL_PROCESS).isPresent();
    }

    private boolean isRoleBasedProfile(Profile profile) {
        return profile.property(ROLE_ARN).isPresent() &&
                profile.property(SOURCE_PROFILE).isPresent();
    }
}
