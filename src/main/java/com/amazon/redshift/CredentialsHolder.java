package com.amazon.redshift;

import java.util.Date;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;

public class CredentialsHolder implements AWSCredentials
{
    protected AWSCredentials m_credentials;
    private Date m_expiration;
    private IamMetadata m_metadata;

    protected CredentialsHolder(AWSCredentials credentials)
    {
        this(credentials, new Date(System.currentTimeMillis() + 15 * 60 * 1000));
    }

    protected CredentialsHolder(AWSCredentials credentials, Date expiration)
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

    public static CredentialsHolder newInstance(AWSCredentials credentials)
    {
        if (credentials instanceof AWSSessionCredentials)
        {
            return new SessionCredentialsHolder(credentials);
        }

        return new CredentialsHolder(credentials);
    }

    public static CredentialsHolder newInstance(AWSCredentials credentials, Date expiration)
    {
        if (credentials instanceof AWSSessionCredentials)
        {
            return new SessionCredentialsHolder(credentials, expiration);
        }

        return new CredentialsHolder(credentials, expiration);
    }

    /**
     * @return The AWS Access Key ID for this credentials object.
     */
    @Override
    public String getAWSAccessKeyId()
    {
        return m_credentials.getAWSAccessKeyId();
    }

    @Override
    public String getAWSSecretKey()
    {
        return m_credentials.getAWSSecretKey();
    }

    public boolean isExpired()
    {
        return m_expiration != null && m_expiration.before(new Date(System.currentTimeMillis() - 60 * 1000 * 5));
    }

    public Date getExpiration()
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

    public void setMetadata(IamMetadata metadata)
    {
        this.m_metadata = metadata;
    }

    private static final class SessionCredentialsHolder
        extends CredentialsHolder
        implements AWSSessionCredentials
    {
        protected SessionCredentialsHolder(AWSCredentials credentials)
        {
            super(credentials);
        }

        protected SessionCredentialsHolder(AWSCredentials credentials, Date expiration)
        {
            super(credentials, expiration);
        }

        @Override
        public String getSessionToken()
        {
            return ((AWSSessionCredentials) m_credentials).getSessionToken();
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
