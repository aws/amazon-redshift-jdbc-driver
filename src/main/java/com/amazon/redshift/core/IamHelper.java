package com.amazon.redshift.core;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.redshift.AmazonRedshift;
import com.amazonaws.services.redshift.AmazonRedshiftClientBuilder;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;
import com.amazonaws.services.redshift.model.Endpoint;
import com.amazonaws.services.redshift.model.GetClusterCredentialsRequest;
import com.amazonaws.services.redshift.model.GetClusterCredentialsResult;
import com.amazonaws.services.redshiftinternal.AmazonRedshiftInternal;
import com.amazonaws.services.redshiftinternal.AmazonRedshiftInternalClient;
import com.amazonaws.services.redshiftinternal.AmazonRedshiftInternalClientBuilder;
import com.amazonaws.services.redshiftinternal.model.GetClusterCredentialsWithIAMRequest;
import com.amazonaws.services.redshiftinternal.model.GetClusterCredentialsWithIAMResult;
import com.amazonaws.util.StringUtils;

import com.amazon.redshift.AuthMech;
import com.amazon.redshift.CredentialsHolder;
import com.amazon.redshift.IPlugin;
import com.amazon.redshift.RedshiftProperty;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class IamHelper
{
	private static final int MAX_AMAZONCLIENT_RETRY = 5;
	private static final int MAX_AMAZONCLIENT_RETRY_DELAY_MS = 1000;

	private static final String KEY_PREFERRED_ROLE = "preferred_role";
	private static final String KEY_ROLE_SESSION_NAME = "roleSessionName";
	private static final String KEY_ROLE_ARN = "roleArn";

	// Subtype of plugin
	public static final int	SAML_PLUGIN = 1;
	public static final int	JWT_PLUGIN = 2;
	
	// Type of GetClusterCredential API
	public static final int	GET_CLUSTER_CREDENTIALS_V1_API	= 1;
	public static final int	GET_CLUSTER_CREDENTIALS_IAM_V2_API 	= 2;
	public static final int	GET_CLUSTER_CREDENTIALS_SAML_V2_API = 3;
	public static final int	GET_CLUSTER_CREDENTIALS_JWT_V2_API 	= 4;
	
	
	private enum CredentialProviderType {
		NONE,
		PROFILE,
		IAM_KEYS_WITH_SESSION,
		IAM_KEYS,
		PLUGIN
	};

	private static Map<String, GetClusterCredentialsResult> credentialsCache = new HashMap<String, GetClusterCredentialsResult>();
	private static Map<String, GetClusterCredentialsWithIAMResult> credentialsV2Cache = new HashMap<String, GetClusterCredentialsWithIAMResult>();

    private IamHelper()
    {
    }

    /**
     * Helper function to handle IAM connection properties. If any IAM related connection property
     * is specified, all other <b>required</b> IAM properties must be specified too or else it
     * throws an error.
     *
     * @param info                Redshift client settings used to authenticate if connection
     *                            should be granted.
     * @param settings						Redshift IAM settings
     * @param log									Redshift logger
     *
     * @throws RedshiftException  If an error occurs.
     */
    public static void setIAMProperties(Properties info, RedshiftJDBCSettings settings, RedshiftLogger log) 
    										throws RedshiftException
    {
    		try {
	        // IAM requires an SSL connection to work. Make sure that m_authMech is set to
	        // SSL level VERIFY_CA or higher.
	        if (settings.m_authMech == null
	        		|| settings.m_authMech.ordinal() < AuthMech.VERIFY_CA.ordinal())
	        {
	            settings.m_authMech = AuthMech.VERIFY_CA;
	        }
	
	        String clusterId = RedshiftConnectionImpl.getRequiredConnSetting(RedshiftProperty.CLUSTER_IDENTIFIER.getName(), info);
	        String awsRegion = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_REGION.getName(), info);
	        String endpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.ENDPOINT_URL.getName(), info);
	        String stsEndpointUrl = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.STS_ENDPOINT_URL.getName(), info);
	        String userName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.UID.getName(), info);
	        if (userName == null)
	        	userName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.USER.getName(), info);	
	        String password = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PWD.getName(), info);
	        if (password == null)
	        	password = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PASSWORD.getName(), info);
	        
	        String profile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_PROFILE.getName(), info);
	        if(profile == null)
	        	profile = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.AWS_PROFILE.getName().toLowerCase(), info);
	        String iamDuration = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_DURATION.getName(), info);
	        String iamAccessKey = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_ACCESS_KEY_ID.getName(), info);
	
	        String iamSecretKey = RedshiftConnectionImpl.getOptionalConnSetting(
	        				RedshiftProperty.IAM_SECRET_ACCESS_KEY.getName(),
	                info);
	
	        String iamSessionToken = RedshiftConnectionImpl.getOptionalConnSetting(
	        				RedshiftProperty.IAM_SESSION_TOKEN.getName(),
	                info);
	
	        String iamCredentialProvider = RedshiftConnectionImpl.getOptionalConnSetting(
	        				RedshiftProperty.CREDENTIALS_PROVIDER.getName(),
	                info);
	
	        String iamAutoCreate = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.USER_AUTOCREATE.getName(), info);
	        String iamDbUser = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DB_USER.getName(), info);
	        String iamDbGroups = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DB_GROUPS.getName(), info);
	        String iamForceLowercase = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.FORCE_LOWERCASE.getName(), info);        
	        String iamGroupFederation = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.GROUP_FEDERATION.getName(), info);        
	        String dbName = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.DBNAME.getName(), info);
	        
	        String hosts = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.HOST.getName(),info);
	        String ports = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.PORT.getName(),info);
	        String iamDisableCache = RedshiftConnectionImpl.getOptionalConnSetting(RedshiftProperty.IAM_DISABLE_CACHE.getName(), info);
	
	        settings.m_clusterIdentifier = clusterId;
	        if (settings.m_clusterIdentifier.isEmpty())
	        {
	        	RedshiftException err =  new RedshiftException(GT.tr("Missing connection property {0}", 
	        																	RedshiftProperty.CLUSTER_IDENTIFIER.getName()),
	              													RedshiftState.UNEXPECTED_ERROR);         	
	
	        	if(RedshiftLogger.isEnable())
	          	log.log(LogLevel.ERROR, err.toString());
	
	          throw err;
	        }
	
	        // Regions.fromName(string) requires the string to be lower case and in this format:
	        // E.g. "us-west-2"
	        if (null != awsRegion)
	        {
	            settings.m_awsRegion = awsRegion.trim().toLowerCase();
	        }
	
	        if (null != endpointUrl)
	        {
	            settings.m_endpoint = endpointUrl;
	        }
	        else
	        {
	            settings.m_endpoint = System.getProperty("redshift.endpoint-url");
	        }

	        if (null != stsEndpointUrl)
	        {
	            settings.m_stsEndpoint = stsEndpointUrl;
	        }
	        else
	        {
	            settings.m_stsEndpoint = System.getProperty("sts.endpoint-url");
	        }
	        
	        if (null != userName)
	        {
	            settings.m_username = userName;
	        }
	
	        if (null != password)
	        {
	            settings.m_password = password;
	        }
	
	        if (null != profile)
	        {
	            settings.m_profile = profile;
	        }
	
	        if (null != iamDuration)
	        {
	            try
	            {
	                settings.m_iamDuration = Integer.parseInt(iamDuration);
	                if (settings.m_iamDuration < 900 || settings.m_iamDuration > 3600)
	                {
	                	RedshiftException err =  new RedshiftException(GT.tr("Invalid connection property value or type range(900-3600) {0}", 
												RedshiftProperty.IAM_DURATION.getName()),
											RedshiftState.UNEXPECTED_ERROR);         	
	
	                  if(RedshiftLogger.isEnable())
	                		log.log(LogLevel.ERROR, err.toString());
	                	
	                  throw err;
	                }
	            }
	            catch (NumberFormatException e)
	            {
	            	RedshiftException err =  new RedshiftException(GT.tr("Invalid connection property value {0} : {1}", 
										RedshiftProperty.IAM_DURATION.getName(), iamDuration),
									RedshiftState.UNEXPECTED_ERROR, e);         	
	
	            	if(RedshiftLogger.isEnable())
	            		log.log(LogLevel.DEBUG, err.toString());
	            	
	              throw err;
	            }
	        }
	
	        if (null != iamAccessKey)
	        {
	            settings.m_iamAccessKeyID = iamAccessKey;
	        }
	
	        // Because the secret access key should be hidden, and most applications (for example:
	        // SQL Workbench) only hide passwords, Amazon has requested that we allow the
	        // secret access key to be passed as either the IAMSecretAccessKey property or
	        // as a password value.
	        if (null != iamSecretKey)
	        {
	            if (StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID))
	            {
	            	RedshiftException err =  new RedshiftException(GT.tr("Missing connection property {0}", 
										RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
									RedshiftState.UNEXPECTED_ERROR);         	
	
	              if(RedshiftLogger.isEnable())
	            		log.log(LogLevel.ERROR, err.toString());
	            	
	              throw err;
	            }
	
	            settings.m_iamSecretKey = iamSecretKey;
	            if (settings.m_iamSecretKey.isEmpty())
	            {
	                settings.m_iamSecretKey = settings.m_password;
	            }
	        }
	        else
	        {
	            settings.m_iamSecretKey = settings.m_password;
	        }
	
	        if (null != iamSessionToken)
	        {
	            if (StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID))
	            {
	            	RedshiftException err =  new RedshiftException(GT.tr("Missing connection property {0}", 
										RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
									RedshiftState.UNEXPECTED_ERROR);         	
	
	              if(RedshiftLogger.isEnable())
	            		log.log(LogLevel.ERROR, err.toString());
	            	throw err;
	            }
	            settings.m_iamSessionToken = iamSessionToken;
	        }
	
	        if (null != iamCredentialProvider)
	        {
	            settings.m_credentialsProvider = iamCredentialProvider;
	        }
	
	        Enumeration<String> enums = (Enumeration<String>) info.propertyNames();
	        while (enums.hasMoreElements()) {
	          // The given properties are String pairs, so this should be OK.
	          String key = enums.nextElement();
	          String value = info.getProperty(key);
	          key = key.toLowerCase(Locale.getDefault());
	          if (!"*".equals(value))
	          {
	              settings.m_pluginArgs.put(key, value);
	          }
	        }        
	
	        settings.m_autocreate =
	                iamAutoCreate == null ? null : Boolean.valueOf(iamAutoCreate);

	        settings.m_iamDisableCache =
	        		iamDisableCache == null ? false : Boolean.valueOf(iamDisableCache);
	        
	        settings.m_forceLowercase =
	        		    iamForceLowercase == null ? null : Boolean.valueOf(iamForceLowercase);

	        settings.m_groupFederation =
      		    iamGroupFederation == null ? false : Boolean.valueOf(iamGroupFederation);
	        
	        
	        if (null != iamDbUser)
	        {
	            settings.m_dbUser = iamDbUser;
	        }
	
	        settings.m_dbGroups = (iamDbGroups != null)
	            ? Arrays.asList((settings.m_forceLowercase != null && settings.m_forceLowercase ? iamDbGroups.toLowerCase(Locale.getDefault()) : iamDbGroups).split(","))            		
	            : Collections.<String>emptyList();
	            
	        settings.m_Schema = dbName;
	        if (hosts != null) {
	        	settings.m_host = hosts;
	        }
	        if (ports != null) {
	        	settings.m_port = Integer.parseInt(ports);
	        }
	
	        setIAMCredentials(settings, log);
    		}
    		catch (RedshiftException re) {
          if(RedshiftLogger.isEnable())
        		log.logError(re);
    			
          throw re;
    		}
    }

    /**
     * Helper function to create the appropriate credential providers.
     *
     * @throws RedshiftException    If an unspecified error occurs.
     */
    private static void setIAMCredentials(RedshiftJDBCSettings settings, RedshiftLogger log) throws RedshiftException
    {
        AWSCredentialsProvider provider;
        CredentialProviderType providerType = CredentialProviderType.NONE;
        boolean idpCredentialsRefresh = false;
    		String idpToken = null;

        if (!StringUtils.isNullOrEmpty(settings.m_credentialsProvider))
        {
            if (!StringUtils.isNullOrEmpty(settings.m_profile))
            {
            	RedshiftException err = new RedshiftException(GT.tr("Conflict in connection property setting {0} and {1}",
										RedshiftProperty.CREDENTIALS_PROVIDER.getName(),
										RedshiftProperty.AWS_PROFILE.getName()),
            			RedshiftState.UNEXPECTED_ERROR);
            	
              if(RedshiftLogger.isEnable())
            		log.log(LogLevel.ERROR, err.toString());
            	
              throw err;
            }

            if (!StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID))
            {
            	RedshiftException err = new RedshiftException(GT.tr("Conflict in connection property setting {0} and {1}",
										RedshiftProperty.CREDENTIALS_PROVIDER.getName(),
										RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
            			RedshiftState.UNEXPECTED_ERROR);
            	
              if(RedshiftLogger.isEnable())
            		log.log(LogLevel.ERROR, err.toString());

              throw err;
            }

            try
            {
                Class<? extends AWSCredentialsProvider> clazz =
                        (Class.forName(settings.m_credentialsProvider)
                                .asSubclass(AWSCredentialsProvider.class));

                provider = clazz.newInstance();
                if (provider instanceof IPlugin)
                {
                    IPlugin plugin = ((IPlugin) provider);
                    
                    providerType = CredentialProviderType.PLUGIN;
                    plugin.setLogger(log);
                    plugin.setGroupFederation(settings.m_groupFederation);
                    for (Map.Entry<String, String> entry : settings.m_pluginArgs.entrySet())
                    {
                    		String pluginArgKey = entry.getKey();
                        plugin.addParameter(pluginArgKey, entry.getValue());
                        
                        if (KEY_PREFERRED_ROLE.equalsIgnoreCase(pluginArgKey))
                        	settings.m_preferredRole = entry.getValue();
                        else if (KEY_ROLE_ARN.equalsIgnoreCase(pluginArgKey))
                        	settings.m_roleArn = entry.getValue();
                        else if (KEY_ROLE_SESSION_NAME.equalsIgnoreCase(pluginArgKey))
                        	settings.m_roleSessionName = entry.getValue();
                        else if (RedshiftProperty.DB_GROUPS_FILTER.getName().equalsIgnoreCase(pluginArgKey))
                        	settings.m_dbGroupsFilter = entry.getValue();
                    }
                }
            }
            catch (InstantiationException 
            				| IllegalAccessException
            				| ClassNotFoundException e)
            {
            	RedshiftException err = new RedshiftException(GT.tr("Invalid credentials provider class {0}",
            			settings.m_credentialsProvider),
            			RedshiftState.UNEXPECTED_ERROR, e);
            	
              if(RedshiftLogger.isEnable())
            		log.log(LogLevel.ERROR, err.toString());
            	
                throw err;
            }
            catch (NumberFormatException e)
            {
            	RedshiftException err = new RedshiftException(GT.tr("{0} : {1}",
            			e.getMessage(),
            			settings.m_credentialsProvider),
            			RedshiftState.UNEXPECTED_ERROR, e);
            	
              if(RedshiftLogger.isEnable())
            		log.log(LogLevel.ERROR, err.toString());
            	
              throw err;
            }
        }
        else if (!StringUtils.isNullOrEmpty(settings.m_profile))
        {
            if (!StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID))
            {
            	RedshiftException err = new RedshiftException(GT.tr("Conflict in connection property setting {0} and {1}",
										RedshiftProperty.AWS_PROFILE.getName(),
										RedshiftProperty.IAM_ACCESS_KEY_ID.getName()),
            			RedshiftState.UNEXPECTED_ERROR);
            	
              if(RedshiftLogger.isEnable())
            		log.log(LogLevel.ERROR, err.toString());
            	
              throw err;
            }

            ProfilesConfigFile pcf = new PluginProfilesConfigFile(settings, log);
            provider = new ProfileCredentialsProvider(pcf, settings.m_profile);
            providerType = CredentialProviderType.PROFILE;
        }
        else if (!StringUtils.isNullOrEmpty(settings.m_iamAccessKeyID))
        {
            AWSCredentials credentials;

            if (!StringUtils.isNullOrEmpty(settings.m_iamSessionToken))
            {
                credentials = new BasicSessionCredentials(
                        settings.m_iamAccessKeyID,
                        settings.m_iamSecretKey,
                        settings.m_iamSessionToken);
                providerType = CredentialProviderType.IAM_KEYS_WITH_SESSION;
            }
            else
            {
                credentials = new BasicAWSCredentials(
                        settings.m_iamAccessKeyID,
                        settings.m_iamSecretKey);
                providerType = CredentialProviderType.IAM_KEYS;
            }

            provider = new AWSStaticCredentialsProvider(credentials);
        }
        else
        {
            provider = new DefaultAWSCredentialsProviderChain();
        }

        if (RedshiftLogger.isEnable())
          log.log(LogLevel.DEBUG, "IDP Credential Provider {0}:{1}", provider, settings.m_credentialsProvider);
        
            
        int getClusterCredentialApiType = findTypeOfGetClusterCredentialsAPI(settings, providerType, provider);
        
        if(getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API
        		|| getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API)
      	{
          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "Calling provider.getCredentials()");
        	
	        // Provider will cache the credentials, it's OK to call getCredentials() here.
	        AWSCredentials credentials = provider.getCredentials();
	        if (credentials instanceof CredentialsHolder)
	        {
	        		idpCredentialsRefresh = ((CredentialsHolder) credentials).isRefresh();
	        	
	            // autoCreate, user and password from URL take priority.
	            CredentialsHolder.IamMetadata im = ((CredentialsHolder) credentials).getMetadata();
	            if (null != im)
	            {
	                Boolean autoCreate = im.getAutoCreate();
	                String dbUser = im.getDbUser();
	                String samlDbUser = im.getSamlDbUser();
	                String profileDbUser = im.getProfileDbUser();
	                String dbGroups = im.getDbGroups();
	                boolean forceLowercase = im.getForceLowercase();                
	                boolean allowDbUserOverride = im.getAllowDbUserOverride();
	                if (null == settings.m_autocreate)
	                {
	                    settings.m_autocreate = autoCreate;
	                }
	                
	                if (null == settings.m_forceLowercase)
		              {
	                		settings.m_forceLowercase = forceLowercase;
		              }
	                
	
	                /*
	                 * Order of precedence when configuring settings.m_dbUser:
	                 *
	                 * If allowDbUserOverride = true:
	                 *      1. Value from SAML assertion.
	                 *      2. Value from connection string setting.
	                 *      3. Value from credentials profile setting.
	                 *
	                 * If allowDbUserOverride = false (default):
	                 *      1. Value from connection string setting.
	                 *      2. Value from credentials profile setting.
	                 *      3. Value from SAML assertion.
	                 */
	                if (allowDbUserOverride)
	                {
	                    if(null != samlDbUser)
	                    {
	                        settings.m_dbUser = samlDbUser;
	                    }
	                    else if(null != dbUser)
	                    {
	                        settings.m_dbUser = dbUser;
	                    }
	                    else if(null != profileDbUser)
	                    {
	                        settings.m_dbUser = profileDbUser;
	                    }
	                }
	                else
	                {
	                    if (null != dbUser)
	                    {
	                        settings.m_dbUser = dbUser;
	                    }
	                    else if (null != profileDbUser)
	                    {
	                        settings.m_dbUser = profileDbUser;
	                    }
	                    else if (null != samlDbUser)
	                    {
	                        settings.m_dbUser = samlDbUser;
	                    }
	                }
	
	                if (settings.m_dbGroups.isEmpty() && null != dbGroups)
	                {
	                    settings.m_dbGroups = Arrays.asList((settings.m_forceLowercase ? dbGroups.toLowerCase(Locale.getDefault()) : dbGroups).split(","));
	                }
	            }
	        }

	        if ("*".equals(settings.m_username) && null == settings.m_dbUser)
	        {
	        	RedshiftException err =  new RedshiftException(GT.tr("Missing connection property {0}", 
								RedshiftProperty.DB_USER.getName()),
							RedshiftState.UNEXPECTED_ERROR);         	
	
	          if(RedshiftLogger.isEnable())
							log.log(LogLevel.ERROR, err.toString());
	        	
	          throw err;
	        }
        } // V1 Or IAM_V2 API
        else {
          if (RedshiftLogger.isEnable())
            log.log(LogLevel.DEBUG, "groupFederation=" + settings.m_groupFederation);
        	
        	// Check for GetClusterCredentialsV2 cache
        	// Combine key of IDP and V2 API
      		String key = null;
      		GetClusterCredentialsWithIAMResult credentials = null;
      		
      		if(!settings.m_iamDisableCache) {
      			key = getCredentialsV2CacheKey(settings, providerType, provider, getClusterCredentialApiType);
      			credentials = credentialsV2Cache.get(key);
      		}
      		
      		if (credentials == null
      				|| credentials.getExpiration().before(new Date(System.currentTimeMillis() - 60 * 1000 * 5)))
      		{
      			// If not found or expired
        		// Get IDP token
        		if(providerType == CredentialProviderType.PLUGIN) {
        			IPlugin plugin = (IPlugin)provider;
        			
              if (RedshiftLogger.isEnable())
                log.log(LogLevel.DEBUG, "Calling plugin.getIdpToken()");
              
        			idpToken = plugin.getIdpToken();
        		}
        		
            settings.m_idpToken = idpToken;
      		}
        } // Group federation API
        

        setClusterCredentials(provider, settings, log, 
        											providerType, idpCredentialsRefresh,
        											getClusterCredentialApiType);
    }

    /**
     * Calls the AWS SDK methods to return temporary credentials.
     * The expiration date is returned as the local time set by the client machines OS.
     *
     * @throws RedshiftException   If getting the cluster credentials fails.
     */
    private static void setClusterCredentials(AWSCredentialsProvider credProvider, 
    															RedshiftJDBCSettings settings, 
    															RedshiftLogger log,
    															CredentialProviderType providerType,
    															boolean idpCredentialsRefresh,
    															int getClusterCredentialApiType)
    					throws RedshiftException
    {
        try
        {
            // Call V1 or V2 GetClusterCredentials API
            if(getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API)
            {
            	// Call V1 API
              AmazonRedshiftClientBuilder builder = AmazonRedshiftClientBuilder.standard();

        	    ClientConfiguration clientConfig = RequestUtils.getProxyClientConfig(log);
        	    
        	    if (clientConfig != null) {
        	    	builder.setClientConfiguration(clientConfig);
        	    }
              
              if (settings.m_endpoint != null)
              {
                  EndpointConfiguration cfg = new EndpointConfiguration(
                          settings.m_endpoint, settings.m_awsRegion);
                  builder.setEndpointConfiguration(cfg);
              }
              else if (settings.m_awsRegion != null && !settings.m_awsRegion.isEmpty())
              {
                  builder.setRegion(settings.m_awsRegion);
              }

              AmazonRedshift client 
              	= (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API
                		|| getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API)
              			? builder.withCredentials(credProvider).build()
              			: builder.build();

              if (null == settings.m_host || settings.m_port == 0)
              {
              		if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API
                  		|| getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API) 
              		{
  	                DescribeClustersRequest req = new DescribeClustersRequest();
  	                req.setClusterIdentifier(settings.m_clusterIdentifier);
  	                DescribeClustersResult resp = client.describeClusters(req);
  	                List<Cluster> clusters = resp.getClusters();
  	                if (clusters.isEmpty())
  	                {
  	                    throw new AmazonClientException("Failed to describeClusters.");
  	                }
  	
  	                Cluster cluster = clusters.get(0);
  	                Endpoint endpoint = cluster.getEndpoint();
  	                if (null == endpoint)
  	                {
  	                    throw new AmazonClientException("Cluster is not fully created yet.");
  	                }
  	
  	                settings.m_host = endpoint.getAddress();
  	                settings.m_port = endpoint.getPort();
              		}
              		else {
                    throw new AmazonClientException("Host or Port parameter is missing.");
              		}
              }
            	
           	
              if (RedshiftLogger.isEnable())
                log.log(LogLevel.DEBUG, "Call V1 API of GetClusterCredentials");
            	
	            GetClusterCredentialsResult result = getClusterCredentialsResult(settings, client, log, providerType, idpCredentialsRefresh);
	            settings.m_username = result.getDbUser();
	            settings.m_password = result.getDbPassword();
	            if(RedshiftLogger.isEnable()) {
	                Date now = new Date();
	                log.logInfo(now + ": Using GetClusterCredentialsResult with expiration " + result.getExpiration());
	            }
            }
            else {
            	// Call V2 API
            	
            	AmazonRedshiftInternalClientBuilder builder = AmazonRedshiftInternalClientBuilder.standard();

        	    ClientConfiguration clientConfig = RequestUtils.getProxyClientConfig(log);
        	    
        	    if (clientConfig != null) {
        	    	builder.setClientConfiguration(clientConfig);
        	    }
              
              if (settings.m_endpoint != null)
              {
                  EndpointConfiguration cfg = new EndpointConfiguration(
                          settings.m_endpoint, settings.m_awsRegion);
                  builder.setEndpointConfiguration(cfg);
              }
              else if (settings.m_awsRegion != null && !settings.m_awsRegion.isEmpty())
              {
                  builder.setRegion(settings.m_awsRegion);
              }
            	
            	AmazonRedshiftInternalClient client 
            	= (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_V1_API
              		|| getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API)
            			? (AmazonRedshiftInternalClient) builder.withCredentials(credProvider).build()
            			: (AmazonRedshiftInternalClient) builder.build();
            	
              if (RedshiftLogger.isEnable())
                log.log(LogLevel.DEBUG, "Call V2 API of GetClusterCredentials");
              
              GetClusterCredentialsWithIAMResult result = getClusterCredentialsResultV2(settings, client, 
	            																						log, providerType, idpCredentialsRefresh,
	            																						credProvider,
	            																						getClusterCredentialApiType);
	            settings.m_username = result.getDbUser();
	            settings.m_password = result.getDbPassword();
	            
	            // result will contain TimeToRefresh
	            
	            if(RedshiftLogger.isEnable()) {
	                Date now = new Date();
	                log.logInfo(now + ": Using GetClusterCredentialsResultV2 with expiration " + result.getExpiration());
	                log.logInfo(now + ": Using GetClusterCredentialsResultV2 with TimeToRefresh " + result.getNextRefreshTime());
	            }
            }
        }
        catch (AmazonClientException e)
        {
        	RedshiftException err =  new RedshiftException(GT.tr("IAM error retrieving temp credentials: {0}",
										e.getMessage()),
        					RedshiftState.UNEXPECTED_ERROR,e);

          if(RedshiftLogger.isEnable())
						log.log(LogLevel.ERROR, err.toString());

          throw err;
        }
    }

    private static synchronized GetClusterCredentialsResult getClusterCredentialsResult(RedshiftJDBCSettings settings, 
    															AmazonRedshift client, 
    															RedshiftLogger log,
    															CredentialProviderType providerType,
    															boolean idpCredentialsRefresh) throws AmazonClientException
    {
        String key = null;
        GetClusterCredentialsResult credentials = null;
        		
        if(!settings.m_iamDisableCache) {
	        key = getCredentialsCacheKey(settings, providerType);
	        credentials = credentialsCache.get(key);
        }

        if (credentials == null
        			|| (providerType == CredentialProviderType.PLUGIN
        						&& idpCredentialsRefresh)
        			|| credentials.getExpiration().before(new Date(System.currentTimeMillis() - 60 * 1000 * 5)))
        {
	          if (RedshiftLogger.isEnable())
	            log.logInfo("GetClusterCredentials NOT from cache");
        	
	          if(!settings.m_iamDisableCache)	          
	          	credentialsCache.remove(key);
	          
            GetClusterCredentialsRequest request = new GetClusterCredentialsRequest();
            request.setClusterIdentifier(settings.m_clusterIdentifier);
            if (settings.m_iamDuration > 0)
            {
                request.setDurationSeconds(settings.m_iamDuration);
            }

            request.setDbName(settings.m_Schema);
            request.setDbUser(settings.m_dbUser == null ? settings.m_username : settings.m_dbUser);
            request.setAutoCreate(settings.m_autocreate);
            request.setDbGroups(settings.m_dbGroups);

            if (RedshiftLogger.isEnable())
                log.logInfo(request.toString());

            for (int i = 0; i < MAX_AMAZONCLIENT_RETRY; ++i)
            {
                try
                {
                    credentials = client.getClusterCredentials(request);
                    break;
                } catch (AmazonClientException ace)
                {
                    if (ace.getMessage().contains("Rate exceeded") && i < MAX_AMAZONCLIENT_RETRY-1)
                    {
                        if(RedshiftLogger.isEnable())
                            log.logInfo("getClusterCredentialsResult caught 'Rate exceeded' error...");
                        try
                        {
                            Thread.sleep(MAX_AMAZONCLIENT_RETRY_DELAY_MS);
                        }
                        catch (InterruptedException ex)
                        {
                            Thread.currentThread().interrupt();
                        }
                        continue;
                    } else
                    {
                        throw ace;
                    }
                }
            }

            if(!settings.m_iamDisableCache)
            	credentialsCache.put(key, credentials);
        }
        else {
          if (RedshiftLogger.isEnable())
            log.logInfo("GetClusterCredentials from cache");
        }
        
    return credentials;
  }

  private static synchronized GetClusterCredentialsWithIAMResult getClusterCredentialsResultV2(RedshiftJDBCSettings settings, 
				AmazonRedshiftInternalClient  client, 
				RedshiftLogger log,
				CredentialProviderType providerType,
				boolean idpCredentialsRefresh,
				AWSCredentialsProvider provider,
				int getClusterCredentialApiType) throws AmazonClientException
  {
		String key = null;
		GetClusterCredentialsWithIAMResult credentials = null;
		
		if(!settings.m_iamDisableCache) {
			key = getCredentialsV2CacheKey(settings, providerType, provider, getClusterCredentialApiType);
			credentials = credentialsV2Cache.get(key);
		}
		
		if (credentials == null
  			|| (providerType == CredentialProviderType.PLUGIN
							&& settings.m_idpToken != null)
				|| credentials.getExpiration().before(new Date(System.currentTimeMillis() - 60 * 1000 * 5)))
		{
			if (RedshiftLogger.isEnable())
				log.logInfo("GetClusterCredentialsV2 NOT from cache");
			
			GetClusterCredentialsWithIAMRequest iamRequest = null;
			
			if(!settings.m_iamDisableCache)	          
				credentialsV2Cache.remove(key);
			
			if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API) {			
				GetClusterCredentialsWithIAMRequest request = new GetClusterCredentialsWithIAMRequest();
				
				iamRequest = request;
				
				request.setClusterIdentifier(settings.m_clusterIdentifier);
				if (settings.m_iamDuration > 0)
				{
					request.setDurationSeconds(settings.m_iamDuration);
				}
				
				request.setDbName(settings.m_Schema);
				
				if (RedshiftLogger.isEnable())
					log.logInfo(request.toString());
			}
			else 
			if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_SAML_V2_API) {			
/*				GetClusterCredentialsWithSAMLRequest request = new GetClusterCredentialsWithSAMLRequest();
				
				request.setClusterIdentifier(settings.m_clusterIdentifier);
				if (settings.m_iamDuration > 0)
				{
					request.setDurationSeconds(settings.m_iamDuration);
				}
				
				request.setDbName(settings.m_Schema);
				
				// SAML specific
				request.setSamlAssertion(settings.m_idpToken);

				if(settings.m_preferredRole != null) 
					request.roleArn(settings.m_preferredRole);
				
				if(settings.m_dbGroupsFilter != null) 
					request.excludeDbGroups(settings.m_dbGroupsFilter);
*/					
			}
			else
			if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_JWT_V2_API) {			
/*				GetClusterCredentialsWithWebIdentityRequest request = new GetClusterCredentialsWithWebIdentityRequest();
				request.setClusterIdentifier(settings.m_clusterIdentifier);
				if (settings.m_iamDuration > 0)
				{
					request.setDurationSeconds(settings.m_iamDuration);
				}
				
				request.setDbName(settings.m_Schema);
				
				// JWT specific
				request.setWebIdentityToken(settings.m_idpToken);
				request.roleArn(settings.m_roleArn);
				
				if(settings.m_roleSessionName != null) 
					request.roleSessionName(settings.m_roleSessionName);
*/					
			}
			
			
		
			for (int i = 0; i < MAX_AMAZONCLIENT_RETRY; ++i)
			{
				try
				{
					// TODO: Remove below comments once CP is ready
					if(getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_IAM_V2_API)
						 credentials = client.getClusterCredentialsWithIAM(iamRequest);
/*					else
					if(getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_SAML_V2_API)
						 credentials = client.getClusterCredentialsWithSAML(request);
					else
					if(getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_JWT_V2_API)
						 credentials = client.getClusterCredentialsWithWebIdentity(request);
*/						 
					
					break;
				} 
				catch (AmazonClientException ace)
				{
					if (ace.getMessage().contains("Rate exceeded") && i < MAX_AMAZONCLIENT_RETRY-1)
					{
						if(RedshiftLogger.isEnable())
						  log.logInfo("getClusterCredentialsResultV2 caught 'Rate exceeded' error...");
						try
						{
						  Thread.sleep(MAX_AMAZONCLIENT_RETRY_DELAY_MS);
						}
						catch (InterruptedException ex)
						{
						  Thread.currentThread().interrupt();
						}
						continue;
					} 
					else
					{
						throw ace;
					}
				}
			}

			if(!settings.m_iamDisableCache)
				credentialsV2Cache.put(key, credentials);
		}
		else 
		{
			if (RedshiftLogger.isEnable())
				log.logInfo("GetClusterCredentialsV2 from cache");
		}
	
		return credentials;
	}
    
	private static String getCredentialsCacheKey(RedshiftJDBCSettings settings, CredentialProviderType providerType)
	{
				String key;
        String dbGroups = "";
        
        if (settings.m_dbGroups != null && !settings.m_dbGroups.isEmpty())
        {
            Collections.sort(settings.m_dbGroups);
            dbGroups = String.join(",", settings.m_dbGroups);
        }
        
        key = settings.m_clusterIdentifier + ";" +
                (settings.m_dbUser == null ? settings.m_username : settings.m_dbUser) + ";" +
                (settings.m_Schema == null ? "" : settings.m_Schema) + ";" +
                dbGroups + ";" +
                settings.m_autocreate + ";" +
                settings.m_iamDuration;
        
        switch (providerType) {
	        case PROFILE: {
	        	key += ";" + settings.m_profile; 
	        	break;
	        }
	        
	        case IAM_KEYS_WITH_SESSION: {
	        	key += ";" + settings.m_iamAccessKeyID
	        				 + ";" + settings.m_iamSecretKey
	        				 + ";" + settings.m_iamSessionToken
	        				 ; 
	        	break;
	        }

	        case IAM_KEYS: {
	        	key += ";" + settings.m_iamAccessKeyID
	        				 + ";" + settings.m_iamSecretKey
	        				 ; 
	        	break;
	        }
	        
	        default: {
	        	break;
	        }
	        
        } // Switch
        
      return key;
	}
	
	private static String getCredentialsV2CacheKey(RedshiftJDBCSettings settings,
					CredentialProviderType providerType,
					AWSCredentialsProvider provider,
					int getClusterCredentialApiType)
	{
			String key = "";
			
			if (providerType == CredentialProviderType.PLUGIN)
			{
				// Get IDP key
				IPlugin plugin = (IPlugin)provider;
				key = plugin.getCacheKey();
			}
      
      
			// Combine IDP key with V2 API parameters
      
      key += (settings.m_clusterIdentifier + ";" +
              (settings.m_Schema == null ? "" : settings.m_Schema) + ";" +
              settings.m_iamDuration);
      
      if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_SAML_V2_API) {
	      if (settings.m_preferredRole != null) {
	      	key += (settings.m_preferredRole + ";");
	      }
	      
	      if (settings.m_dbGroupsFilter != null) {
	      	key += (settings.m_dbGroupsFilter + ";");
	      }
      }
      else
      if (getClusterCredentialApiType == GET_CLUSTER_CREDENTIALS_JWT_V2_API) {
      	if (settings.m_idpToken != null) {
      		key += (settings.m_idpToken + ";");
      	}
      	
        if (settings.m_roleArn != null) {
        	key += (settings.m_roleArn + ";");
        }
        
        if (settings.m_roleSessionName != null) {
        	key += (settings.m_roleSessionName + ";");
        }
      }
      
      switch (providerType) {
        case PROFILE: {
        	key += ";" + settings.m_profile; 
        	break;
        }
        
        case IAM_KEYS_WITH_SESSION: {
        	key += ";" + settings.m_iamAccessKeyID
        				 + ";" + settings.m_iamSecretKey
        				 + ";" + settings.m_iamSessionToken
        				 ; 
        	break;
        }

        case IAM_KEYS: {
        	key += ";" + settings.m_iamAccessKeyID
        				 + ";" + settings.m_iamSecretKey
        				 ; 
        	break;
        }
        
        default: {
        	break;
        }
        
      } // Switch
      
    return key;
	}
	
	private static int findTypeOfGetClusterCredentialsAPI(RedshiftJDBCSettings settings,
			CredentialProviderType providerType,
			AWSCredentialsProvider provider) {
    if(!settings.m_groupFederation) 
    	return GET_CLUSTER_CREDENTIALS_V1_API;
    else {
    	if(providerType == CredentialProviderType.PROFILE) {
    		// profile may have role based and it's not supported in V2 API
				throw new AmazonClientException("Authentication with profile is not supported for group federation");					
    	}
    	else
  		if(providerType != CredentialProviderType.PLUGIN)
  				return GET_CLUSTER_CREDENTIALS_IAM_V2_API;
  		else {
				IPlugin plugin = (IPlugin)provider;
				if(plugin.getSubType() == SAML_PLUGIN)
					return GET_CLUSTER_CREDENTIALS_SAML_V2_API;
				else
				if(plugin.getSubType() == JWT_PLUGIN)
					return GET_CLUSTER_CREDENTIALS_JWT_V2_API;
				else
					throw new AmazonClientException("Invalid plugin sub type:" + plugin.getSubType());					
  		}
    }
	}
}
