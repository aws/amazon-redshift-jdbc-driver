package com.amazon.redshift;

import java.time.Instant;

import com.amazon.redshift.plugin.utils.RequestUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

public class CredentialsHolder implements AwsCredentials
{
    protected AwsCredentials m_credentials;
    private Instant m_expiration;
    private IamMetadata m_metadata;
    private boolean refresh; // true means newly added, false means from cache.

    protected CredentialsHolder(AwsCredentials credentials)
    {
        this(credentials, Instant.now().plusSeconds(15 * 60));
    }

    protected CredentialsHolder(AwsCredentials credentials, Instant expiration)
    {
        this.m_credentials = credentials;

        if (credentials instanceof CredentialsHolder)
        {
            CredentialsHolder h = (CredentialsHolder) credentials;
            this.m_metadata = h.getMetadata();
            this.m_expiration = h.getExpiration();
        }
        else
        {
            this.m_expiration = expiration;
        }
    }

    public static CredentialsHolder newInstance(AwsCredentials credentials)
    {
        if (credentials instanceof AwsSessionCredentials)
        {
            return new SessionCredentialsHolder(credentials);
        }

        return new CredentialsHolder(credentials);
    }

    public static CredentialsHolder newInstance(AwsCredentials credentials, Instant expiration)
    {
        if (credentials instanceof AwsSessionCredentials)
        {
            return new SessionCredentialsHolder(credentials, expiration);
        }
        return new CredentialsHolder(credentials, expiration);
    }

    @Override
    public String accessKeyId()
    {
        return m_credentials.accessKeyId();
    }

    @Override
    public String secretAccessKey()
    {
        return m_credentials.secretAccessKey();
    }

    public boolean isExpired()
    {
        return RequestUtils.isCredentialExpired(m_expiration);
    }

    public Instant getExpiration()
    {
        return m_expiration;
    }

    public IamMetadata getMetadata()
    {
        if (m_metadata == null)
        {
            return null;
        }
        return m_metadata.clone();
    }

    public IamMetadata getThisMetadata()
    {
        if (m_metadata == null)
        {
            return null;
        }
        return m_metadata;
    }

    public void setRefresh(boolean flag)
    {
        refresh = flag;
    }

    public boolean isRefresh()
    {
        return refresh;
    }

    public void setMetadata(IamMetadata metadata)
    {
        this.m_metadata = metadata;
    }

    private static final class SessionCredentialsHolder
            extends CredentialsHolder implements AwsSessionCredentialsIdentity
    {

        private SessionCredentialsHolder(AwsCredentials credentials)
        {
            super(credentials);
        }

        private SessionCredentialsHolder(AwsCredentials credentials, Instant expiration)
        {
            super(credentials, expiration);
        }

        @Override
        public String sessionToken()
        {
            return ((AwsSessionCredentials) m_credentials).sessionToken();
        }
    }

    public static final class IamMetadata implements Cloneable
    {
        private Boolean autoCreate;
        /**
         * Connection string setting.
         */
        private String dbUser;

        /**
         * Value from SAML assertion.
         */
        private String samlDbUser;
        /**
         * Connection profile setting.
         */
        private String profileDbUser;

        private String dbGroups;

        /**
         * Property set by the datasource. We extract its value from SAML response.
         * If it's true, the dbUser in SAML response overwrites the dbUser passed in connection
         * string.
         */
        private boolean allowDbUserOverride = false;

        /**
         * Forces the passed in dbGroups setting to be lower case.
         */
        private boolean forceLowercase = false;
        public Boolean getAutoCreate()
        {
            return autoCreate;
        }

        public String getDbUser()
        {
            return dbUser;
        }

        public void setDbUser(String dbUser)
        {
            this.dbUser = dbUser;
        }

        public void setAutoCreate(Boolean autoCreate)
        {
            this.autoCreate = autoCreate;
        }

        public String getSamlDbUser()
        {
            return samlDbUser;
        }

        public void setSamlDbUser(String dbUser)
        {
            this.samlDbUser = dbUser;
        }

        public String getProfileDbUser()
        {
            return profileDbUser;
        }

        public void setProfileDbUser(String dbUser)
        {
            this.profileDbUser = dbUser;
        }

        public String getDbGroups()
        {
            return dbGroups;
        }

        public void setDbGroups(String dbGroups)
        {
            this.dbGroups = dbGroups;
        }

        public boolean getForceLowercase()
        {
            return forceLowercase;
        }

        public void setForceLowercase(boolean forceLowercase)
        {
            this.forceLowercase = forceLowercase;
        }
        public boolean getAllowDbUserOverride()
        {
            return allowDbUserOverride;
        }

        public void setAllowDbUserOverride(boolean allowDbUserOverride)
        {
            this.allowDbUserOverride = allowDbUserOverride;
        }

        @Override
        public IamMetadata clone()
        {
            try
            {
                return (IamMetadata) super.clone();
            }
            catch (CloneNotSupportedException e)
            {
                return null;
            }
        }
    }
}
