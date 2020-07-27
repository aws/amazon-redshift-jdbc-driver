package com.amazon.redshift.core;

import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.auth.profile.internal.BasicProfile;
import com.amazonaws.auth.profile.internal.ProfileStaticCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.util.StringUtils;
import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.logger.RedshiftLogger;

public class PluginProfilesConfigFile extends ProfilesConfigFile
{
    private Map<String, CredentialsHolder> cache = new ConcurrentHashMap<String, CredentialsHolder>();

    private RedshiftJDBCSettings m_settings;
    private RedshiftLogger m_log;
    private static final String PROFILE_PREFIX = "profile ";    

    public PluginProfilesConfigFile(RedshiftJDBCSettings settings, RedshiftLogger log)
    {
        m_settings = settings;
        m_log = log;
    }

    /**
     * Returns the AWS credentials for the specified profile.
     */
    public CredentialsHolder getCredentials(String profileName)
    {
        CredentialsHolder credentials = cache.get(profileName);
        
        if (credentials == null)
        {
            // in case if profile is in ~/.aws/config file, check for 'profile ' prefix
            credentials = cache.get(PROFILE_PREFIX+profileName);
        }
        
        if (credentials != null && !credentials.isExpired())
        {
            return credentials;
        }

        Map<String, BasicProfile> map = getAllBasicProfiles();
        BasicProfile profile = map.get(profileName);
        if (profile == null)
        {
          // in case if profile is in ~/.aws/config file, check for 'profile ' prefix
          profile = map.get(PROFILE_PREFIX+profileName);
        }
        
        if (profile == null)
        {
            throw new SdkClientException("No AWS profile named '" + profileName + "'");
        }

        if (profile.isRoleBasedProfile())
        {
            String srcProfile = profile.getRoleSourceProfile();
            CredentialsHolder srcCred = getCredentials(srcProfile);
            AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(srcCred);
            credentials = assumeRole(profile, provider);
            credentials.setMetadata(srcCred.getMetadata());
            cache.put(profileName, credentials);
            return credentials;
        }

        String dbUser = null;
        String autoCreate = null;
        String dbGroups = null;
        String forceLowercase = null;
        
        String pluginName = profile.getPropertyValue("plugin_name");
        if (!StringUtils.isNullOrEmpty(pluginName))
        {
            try
            {
                Class<? extends AWSCredentialsProvider> clazz =
                        (Class.forName(pluginName).asSubclass(AWSCredentialsProvider.class));

                AWSCredentialsProvider p = clazz.newInstance();
                if (p instanceof IPlugin)
                {
                    IPlugin plugin = (IPlugin)p;
                    plugin.setLogger(m_log);
                    Map<String, String> prop = profile.getProperties();
                    for (Map.Entry<String, String> entry : prop.entrySet())
                    {
                        String key = entry.getKey().toLowerCase(Locale.getDefault());
                        if (!"plugin_name".equals(key))
                        {
                            String value = entry.getValue();
                            plugin.addParameter(key, value);

                            // DbUser value in connection string
                            if (RedshiftProperty.DB_USER.getName().equalsIgnoreCase(key))
                            {
                                dbUser = value;
                            }
                            else if (RedshiftProperty.DB_GROUPS.getName().equalsIgnoreCase(key))
                            {
                                dbGroups = value;
                            }
                            else if (RedshiftProperty.FORCE_LOWERCASE.getName().equalsIgnoreCase(key))
                            {
                            	forceLowercase = value;
                            }
                            else if (RedshiftProperty.USER_AUTOCREATE.getName().equalsIgnoreCase(key))
                            {
                                autoCreate = value;
                            }
                        }
                    }

                    // Add parameters from URL to plugin, override parameters from profile
                    for (Map.Entry<String, String> entry : m_settings.m_pluginArgs.entrySet())
                    {
                        String key = entry.getKey().toLowerCase(Locale.getDefault());
                        if (!"plugin_name".equals(key))
                        {
                            plugin.addParameter(entry.getKey(), entry.getValue());
                        }
                    }
                }

                credentials = CredentialsHolder.newInstance(p.getCredentials());
            }
            catch (InstantiationException e)
            {
                throw new SdkClientException("Invalid plugin: '" + pluginName + "'");
            }
            catch (IllegalAccessException e)
            {
                throw new SdkClientException("Invalid plugin: '" + pluginName + "'");
            }
            catch (ClassNotFoundException e)
            {
                throw new SdkClientException("Invalid plugin: '" + pluginName + "'");
            }
        }
        else
        {
            AWSCredentials c = new ProfileStaticCredentialsProvider(profile).getCredentials();
            credentials = CredentialsHolder.newInstance(c);
        }

        // override DbUser, AutoCreate , DbGroups, and ForceLowercase if null and defined in profile
        CredentialsHolder.IamMetadata metadata = credentials.getMetadata();
        if (null == metadata)
        {
            metadata = new CredentialsHolder.IamMetadata();
        }

        if (null != dbUser)
        {
            metadata.setProfileDbUser(dbUser);
        }

        if (null != autoCreate)
        {
            metadata.setAutoCreate(Boolean.valueOf(autoCreate));
        }
        if (null != dbGroups)
        {
            metadata.setDbGroups(dbGroups);
        }
        
        if (null != forceLowercase)
        {
            metadata.setForceLowercase(Boolean.valueOf(forceLowercase));
        }
        
        credentials.setMetadata(metadata);

        cache.put(profileName, credentials);
        return credentials;
    }

    private CredentialsHolder assumeRole(BasicProfile profile, AWSCredentialsProvider provider)
    {
        AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard();
        if (!StringUtils.isNullOrEmpty(m_settings.m_awsRegion)) {
            builder.setRegion(m_settings.m_awsRegion);
        }
        AWSSecurityTokenService stsSvc = builder.withCredentials(provider).build();

        String roleArn = profile.getRoleArn();
        String roleSessionName = profile.getRoleSessionName();
        if (StringUtils.isNullOrEmpty(roleSessionName))
        {
            roleSessionName = "redshift-jdbc-" + System.currentTimeMillis();
        }

        String externalId = profile.getRoleExternalId();
        AssumeRoleRequest assumeRoleRequest =
                new AssumeRoleRequest().withRoleArn(roleArn).withRoleSessionName(roleSessionName);

        if (!StringUtils.isNullOrEmpty(externalId))
        {
            assumeRoleRequest = assumeRoleRequest.withExternalId(externalId);
        }

        AssumeRoleResult result = stsSvc.assumeRole(assumeRoleRequest);
        Credentials cred = result.getCredentials();
        Date expiration = cred.getExpiration();

        AWSSessionCredentials c = new BasicSessionCredentials(
                cred.getAccessKeyId(),
                cred.getSecretAccessKey(),
                cred.getSessionToken());

        return CredentialsHolder.newInstance(c, expiration);
    }
}
